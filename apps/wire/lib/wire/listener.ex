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
        {:ok, pid} = Wire.Connection.start(client)
        :gen_tcp.controlling_process(client, pid)

      {:error, :timeout} ->
        :ok

      {:error, reason} ->
        Logger.error("Accept error: #{inspect(reason)}")
    end

    send(self(), :accept)
    {:noreply, state}
  end
end
