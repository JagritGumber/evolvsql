defmodule Wire.Connection do
  @moduledoc "Handles one PostgreSQL client session over the wire protocol v3."
  require Logger

  # Idle connections hibernate after this timeout, freeing their entire heap.
  # The process wakes instantly when the client sends the next message.
  @idle_timeout_ms 30_000

  def start(socket) do
    pid = :erlang.spawn_opt(fn -> handshake(socket) end, [
      :link,
      {:min_heap_size, 233},
      {:min_bin_vheap_size, 46},
      {:fullsweep_after, 10}
    ])
    {:ok, pid}
  end

  # --- Startup ---

  defp handshake(socket) do
    with {:ok, <<len::32>>} <- :gen_tcp.recv(socket, 4),
         {:ok, payload} <- :gen_tcp.recv(socket, len - 4) do
      case payload do
        <<80_877_103::32>> ->
          :gen_tcp.send(socket, "N")
          handshake(socket)

        <<3::16, 0::16, rest::binary>> ->
          params = parse_params(rest)
          Logger.info("connect: #{params["user"]}@#{params["database"]}")
          send_auth_ok(socket)
          send_params(socket)
          send_backend_key(socket)
          send_ready(socket)
          loop(socket)

        _ ->
          send_error(socket, "08P01", "Unsupported protocol")
          :gen_tcp.close(socket)
      end
    else
      {:error, :closed} -> :ok
      {:error, reason} -> Logger.error("Handshake: #{inspect(reason)}")
    end
  end

  defp parse_params(data) do
    data
    |> :binary.split(<<0>>, [:global])
    |> Enum.reject(&(&1 == ""))
    |> Enum.chunk_every(2)
    |> Enum.into(%{}, fn
      [k, v] -> {k, v}
      _ -> {"_", ""}
    end)
  end

  # --- Auth ---

  defp send_auth_ok(s), do: :gen_tcp.send(s, <<"R", 8::32, 0::32>>)

  defp send_params(s) do
    for {k, v} <- [
          {"server_version", "18.0.0"},
          {"server_encoding", "UTF8"},
          {"client_encoding", "UTF8"},
          {"DateStyle", "ISO, MDY"},
          {"integer_datetimes", "on"},
          {"standard_conforming_strings", "on"}
        ] do
      payload = <<k::binary, 0, v::binary, 0>>
      :gen_tcp.send(s, <<"S", byte_size(payload) + 4::32, payload::binary>>)
    end
  end

  defp send_backend_key(s) do
    pid = :erlang.phash2(self(), 0x7FFFFFFF)
    key = :rand.uniform(0x7FFFFFFF)
    :gen_tcp.send(s, <<"K", 12::32, pid::32, key::32>>)
  end

  # --- Query Loop ---
  # Uses recv with timeout. After @idle_timeout_ms of no activity, the
  # process hibernates — heap is freed to zero. It wakes instantly when
  # the client sends the next message. This makes idle connections
  # essentially free (~400 bytes vs ~10-50 KB active).

  defp loop(socket) do
    case :gen_tcp.recv(socket, 5, @idle_timeout_ms) do
      {:ok, <<type, len::32>>} ->
        handle_msg(socket, type, len - 4)

      {:error, :timeout} ->
        enter_hibernate(socket)

      {:error, :closed} ->
        :ok

      {:error, r} ->
        Logger.error("Connection: #{inspect(r)}")
    end
  end

  defp handle_msg(socket, ?Q, body_len) do
    simple_query(socket, body_len)
    :erlang.garbage_collect()
    loop(socket)
  end

  defp handle_msg(socket, ?X, _body_len) do
    :gen_tcp.close(socket)
  end

  defp handle_msg(socket, ?P, body_len) do
    if body_len > 0, do: :gen_tcp.recv(socket, body_len)
    :gen_tcp.send(socket, <<"1", 4::32>>)
    loop(socket)
  end

  defp handle_msg(socket, ?B, body_len) do
    if body_len > 0, do: :gen_tcp.recv(socket, body_len)
    :gen_tcp.send(socket, <<"2", 4::32>>)
    loop(socket)
  end

  defp handle_msg(socket, ?S, body_len) do
    if body_len > 0, do: :gen_tcp.recv(socket, body_len)
    :gen_tcp.send(socket, <<"Z", 5::32, "I">>)
    loop(socket)
  end

  defp handle_msg(socket, _type, body_len) do
    if body_len > 0, do: :gen_tcp.recv(socket, body_len)
    loop(socket)
  end

  # --- Hibernate ---
  # Switch to active:once so the socket delivers data as a message,
  # then hibernate. The BEAM frees the entire process heap.
  # On wake, __wake__/1 is called with a fresh empty heap.

  defp enter_hibernate(socket) do
    :inet.setopts(socket, active: :once)
    :proc_lib.hibernate(__MODULE__, :__wake__, [socket])
  end

  @doc false
  def __wake__(socket) do
    receive do
      {:tcp, ^socket, data} ->
        :inet.setopts(socket, active: false)
        handle_wake_data(socket, data)

      {:tcp_closed, ^socket} ->
        :ok

      {:tcp_error, ^socket, _reason} ->
        :ok
    end
  end

  # Client sent data while we were hibernated. The data may contain a
  # partial or complete PG wire message. Read the 5-byte header, then
  # dispatch normally.
  defp handle_wake_data(socket, data) when byte_size(data) >= 5 do
    <<type, len::32, rest::binary>> = data
    body_len = len - 4
    remaining = body_len - byte_size(rest)

    if remaining > 0 do
      case :gen_tcp.recv(socket, remaining) do
        {:ok, more} ->
          body = <<rest::binary, more::binary>>
          handle_msg_with_body(socket, type, body)
        {:error, :closed} -> :ok
        {:error, r} -> Logger.error("Connection: #{inspect(r)}")
      end
    else
      body = if body_len > 0, do: binary_part(rest, 0, body_len), else: <<>>
      handle_msg_with_body(socket, type, body)
    end
  end

  defp handle_wake_data(socket, partial) do
    need = 5 - byte_size(partial)
    case :gen_tcp.recv(socket, need) do
      {:ok, more} -> handle_wake_data(socket, <<partial::binary, more::binary>>)
      {:error, :closed} -> :ok
      {:error, r} -> Logger.error("Connection: #{inspect(r)}")
    end
  end

  # Dispatch when we already have the full message body
  defp handle_msg_with_body(socket, ?Q, body) do
    sql = String.trim_trailing(body, <<0>>)
    case run_query(String.trim(sql)) do
      {:rows, cols, rows, tag} ->
        buf = [
          encode_row_desc(cols),
          Enum.map(rows, &encode_data_row/1),
          encode_complete(tag),
          <<"Z", 5::32, "I">>
        ]
        :gen_tcp.send(socket, buf)

      {:command, tag} ->
        :gen_tcp.send(socket, [encode_complete(tag), <<"Z", 5::32, "I">>])

      {:error, msg} ->
        :gen_tcp.send(socket, [encode_error("42601", msg), <<"Z", 5::32, "I">>])
    end
    :erlang.garbage_collect()
    loop(socket)
  end

  defp handle_msg_with_body(socket, ?X, _body) do
    :gen_tcp.close(socket)
  end

  defp handle_msg_with_body(socket, ?P, _body) do
    :gen_tcp.send(socket, <<"1", 4::32>>)
    loop(socket)
  end

  defp handle_msg_with_body(socket, ?B, _body) do
    :gen_tcp.send(socket, <<"2", 4::32>>)
    loop(socket)
  end

  defp handle_msg_with_body(socket, ?S, _body) do
    :gen_tcp.send(socket, <<"Z", 5::32, "I">>)
    loop(socket)
  end

  defp handle_msg_with_body(socket, _type, _body) do
    loop(socket)
  end

  # --- Simple Query ---

  defp simple_query(socket, body_len) do
    {:ok, data} = :gen_tcp.recv(socket, body_len)
    sql = String.trim_trailing(data, <<0>>)

    case run_query(String.trim(sql)) do
      {:rows, cols, rows, tag} ->
        buf = [
          encode_row_desc(cols),
          Enum.map(rows, &encode_data_row/1),
          encode_complete(tag),
          <<"Z", 5::32, "I">>
        ]
        :gen_tcp.send(socket, buf)

      {:command, tag} ->
        buf = [encode_complete(tag), <<"Z", 5::32, "I">>]
        :gen_tcp.send(socket, buf)

      {:error, msg} ->
        buf = [encode_error("42601", msg), <<"Z", 5::32, "I">>]
        :gen_tcp.send(socket, buf)
    end
  end

  # --- Query dispatch — routes to Rust engine via NIF ---

  defp run_query(sql) do
    normalized = sql |> String.downcase() |> String.trim() |> String.trim_trailing(";") |> String.trim()

    cond do
      normalized == "select version()" ->
        v = "pgrx 0.1.0 on BEAM/OTP 27 + Rust — PostgreSQL 18.0 compatible"
        {:rows, [{"version", 25}], [[v]], "SELECT 1"}

      normalized == "select current_database()" ->
        {:rows, [{"current_database", 25}], [["pgrx"]], "SELECT 1"}

      normalized == "" ->
        {:command, "EMPTY"}

      true ->
        case Engine.execute_sql(sql) do
          {:ok, %{tag: tag, columns: columns, rows: rows}} ->
            if columns == [] and rows == [] do
              {:command, tag}
            else
              {:rows, columns, rows, tag}
            end

          {:error, msg} ->
            {:error, msg}
        end
    end
  end

  # --- Response Encoding (returns iodata, does NOT send) ---

  defp encode_row_desc(cols) do
    fields =
      for {name, oid} <- cols, into: <<>> do
        <<name::binary, 0, 0::32, 0::16, oid::32, -1::signed-16, -1::signed-32, 0::16>>
      end

    payload = <<length(cols)::16, fields::binary>>
    <<"T", byte_size(payload) + 4::32, payload::binary>>
  end

  defp encode_data_row(vals) do
    fields =
      for v <- vals, into: <<>> do
        case v do
          nil -> <<-1::signed-32>>
          val ->
            bytes = to_string(val)
            <<byte_size(bytes)::32, bytes::binary>>
        end
      end

    payload = <<length(vals)::16, fields::binary>>
    <<"D", byte_size(payload) + 4::32, payload::binary>>
  end

  defp encode_complete(tag) do
    payload = <<tag::binary, 0>>
    <<"C", byte_size(payload) + 4::32, payload::binary>>
  end

  defp encode_error(code, msg) do
    payload = <<"S", "ERROR", 0, "V", "ERROR", 0, "C", code::binary, 0, "M", msg::binary, 0, 0>>
    <<"E", byte_size(payload) + 4::32, payload::binary>>
  end

  defp send_error(s, code, msg) do
    :gen_tcp.send(s, encode_error(code, msg))
  end

  defp send_ready(s), do: :gen_tcp.send(s, <<"Z", 5::32, "I">>)
end
