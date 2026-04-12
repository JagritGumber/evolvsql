defmodule Wire.Listener do
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    port = Keyword.get(opts, :port, 5433)

    tcp_opts = [
      :binary,
      packet: :raw,
      active: false,
      reuseaddr: true,
      nodelay: true,
      backlog: 128,
      buffer: 8192
      # sndbuf/recbuf omitted — let kernel auto-tune
      # PG wire protocol messages are typically < 8KB
    ]

    Wire.ConnCounter.init()

    case :gen_tcp.listen(port, tcp_opts) do
      {:ok, socket} ->
        Logger.info("evolvsql listening on port #{port}")
        send(self(), :accept)
        {:ok, %{socket: socket}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_info(:accept, %{socket: socket} = state) do
    case :gen_tcp.accept(socket, 1000) do
      {:ok, client} ->
        case Wire.ConnCounter.try_acquire() do
          :ok ->
            {:ok, pid} = Wire.Connection.start(client)
            # Monitor the connection so we release the counter on ANY exit path
            # (including hibernate, which destroys the call stack and any try/after).
            Process.monitor(pid)
            :gen_tcp.controlling_process(client, pid)

          {:error, :too_many_connections} ->
            reject_connection(client)
        end

      {:error, :timeout} ->
        :ok

      {:error, reason} ->
        Logger.error("Accept error: #{inspect(reason)}")
    end

    send(self(), :accept)
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    Wire.ConnCounter.release()
    {:noreply, state}
  end

  defp reject_connection(client) do
    # Read and discard the startup message first so the client is in a state
    # where it expects to receive server messages. Skipping this can cause
    # the client to see a connection-reset instead of our FATAL error.
    drain_startup(client)

    msg =
      <<"S", "FATAL", 0, "C", "53300", 0, "M",
        "too many connections", 0, 0>>

    :gen_tcp.send(client, <<"E", byte_size(msg) + 4::32, msg::binary>>)
    :gen_tcp.close(client)
    Logger.warning("Rejected connection: limit reached")
  end

  defp drain_startup(client) do
    with {:ok, <<len::32>>} <- :gen_tcp.recv(client, 4, 5_000),
         {:ok, payload} <- :gen_tcp.recv(client, len - 4, 5_000) do
      case payload do
        # SSL request: respond N (no SSL), then read the actual startup
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
