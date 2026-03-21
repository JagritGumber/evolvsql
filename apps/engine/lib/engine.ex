defmodule Engine do
  defdelegate ping, to: Engine.Native
end
