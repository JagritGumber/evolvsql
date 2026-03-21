defmodule Wire.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    port = Application.get_env(:wire, :port, 5433)

    children = [
      {Wire.Listener, port: port}
    ]

    opts = [strategy: :one_for_one, name: Wire.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
