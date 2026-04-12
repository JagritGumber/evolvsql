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
    # Send PG error response: too many connections
    msg =
      <<"S", "FATAL", 0, "C", "53300", 0, "M",
        "too many connections", 0, 0>>

    :gen_tcp.send(client, <<"E", byte_size(msg) + 4::32, msg::binary>>)
    :gen_tcp.close(client)
    Logger.warning("Rejected connection: limit reached")
  end
end
