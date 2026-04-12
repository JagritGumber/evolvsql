defmodule Wire.Rejector do
  @moduledoc """
  Handles rejected connections asynchronously. Drains the client's
  startup message so the client is in a state that expects server
  messages, then sends a PG FATAL error (SQLSTATE 53300) and closes.

  Runs in a separate process to avoid blocking the listener accept loop
  with synchronous recv/send I/O.
  """
  require Logger

  # Shorter timeout since this is a rejection path; we don't want slow
  # clients to tie up rejection workers.
  @recv_timeout 2_000

  def reject(client) do
    drain_startup(client)

    msg =
      <<"S", "FATAL", 0, "C", "53300", 0, "M",
        "too many connections", 0, 0>>

    :gen_tcp.send(client, <<"E", byte_size(msg) + 4::32, msg::binary>>)
    :gen_tcp.close(client)
    Logger.warning("Rejected connection: limit reached")
  end

  # Read and discard the startup message. Guards against malformed length
  # fields (len < 4) that would cause :gen_tcp.recv to raise on negative
  # length. Handles SSL negotiation by responding "N" and reading again.
  defp drain_startup(client) do
    with {:ok, <<len::32>>} when len >= 4 <- :gen_tcp.recv(client, 4, @recv_timeout),
         {:ok, payload} <- :gen_tcp.recv(client, len - 4, @recv_timeout) do
      case payload do
        <<80_877_103::32>> ->
          :gen_tcp.send(client, "N")
          drain_startup(client)

        _ ->
          :ok
      end
    else
      _ -> :ok
    end
  end
end
