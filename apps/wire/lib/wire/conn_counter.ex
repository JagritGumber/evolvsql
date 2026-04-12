defmodule Wire.ConnCounter do
  @moduledoc """
  Atomic connection counter. Tracks active connections and enforces
  a configurable maximum to prevent resource exhaustion under load.
  """

  @default_max 100

  def init do
    # Idempotent ref creation: only create the atomic ref if one doesn't
    # exist yet. Preserves the ref across supervisor restarts so callers
    # don't race on a nil lookup.
    case :persistent_term.get({__MODULE__, :ref}, nil) do
      nil ->
        ref = :atomics.new(1, signed: true)
        :persistent_term.put({__MODULE__, :ref}, ref)
        :persistent_term.put({__MODULE__, :max}, max_connections())

      _existing ->
        :ok
    end
  end

  @doc """
  Reset the counter to zero. Called by Wire.ConnRegistry on (re)start
  because the registry's monitor state is fresh after any crash, so
  the counter must be reset to match. Accepts a brief window where
  existing orphan connections are over-limit until they terminate.
  """
  def reset do
    ref = :persistent_term.get({__MODULE__, :ref})
    :atomics.put(ref, 1, 0)
    :ok
  end

  def try_acquire do
    ref = :persistent_term.get({__MODULE__, :ref})
    max = :persistent_term.get({__MODULE__, :max})
    new = :atomics.add_get(ref, 1, 1)
    if new > max do
      :atomics.sub(ref, 1, 1)
      {:error, :too_many_connections}
    else
      :ok
    end
  end

  def release do
    ref = :persistent_term.get({__MODULE__, :ref})
    :atomics.sub(ref, 1, 1)
    :ok
  end

  def count do
    ref = :persistent_term.get({__MODULE__, :ref})
    :atomics.get(ref, 1)
  end

  defp max_connections do
    case System.get_env("EVOLVSQL_MAX_CONNECTIONS") do
      nil -> @default_max
      val -> String.to_integer(val)
    end
  end
end
