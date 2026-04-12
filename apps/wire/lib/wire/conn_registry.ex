defmodule Wire.ConnRegistry do
  @moduledoc """
  Owns connection process monitors independently of the listener.
  Surviving listener restarts prevents counter slot leakage: on
  listener crash, the registry keeps monitoring existing connections
  and decrements the counter when they terminate.
  """
  use GenServer

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @doc """
  Register a connection pid for tracking. The registry monitors the
  pid and decrements Wire.ConnCounter when it terminates.
  """
  def track(pid) when is_pid(pid) do
    GenServer.cast(__MODULE__, {:track, pid})
  end

  @impl true
  def init(_) do
    # Initialize counter here so it exists before the listener starts.
    # init is idempotent, so a supervisor restart of registry alone won't
    # reset the counter.
    Wire.ConnCounter.init()
    # Map monitor refs to pids (unused value, kept for potential future lookups)
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:track, pid}, refs) do
    ref = Process.monitor(pid)
    {:noreply, Map.put(refs, ref, pid)}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, refs) do
    Wire.ConnCounter.release()
    {:noreply, Map.delete(refs, ref)}
  end
end
