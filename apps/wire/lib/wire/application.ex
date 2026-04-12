defmodule Wire.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    port = Application.get_env(:wire, :port, 5433)

    # ConnRegistry starts before Listener so monitors survive listener restarts.
    # :rest_for_one ensures that if the registry crashes, the listener restarts
    # too (it needs a running registry to call track/1).
    children = [
      Wire.ConnRegistry,
      {Wire.Listener, port: port}
    ]

    opts = [strategy: :rest_for_one, name: Wire.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
