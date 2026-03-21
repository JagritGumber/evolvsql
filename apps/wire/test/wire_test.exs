defmodule WireTest do
  use ExUnit.Case
  doctest Wire

  test "greets the world" do
    assert Wire.hello() == :world
  end
end
