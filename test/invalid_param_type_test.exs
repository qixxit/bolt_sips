defmodule Bolt.Sips.InvalidParamType.Test do
  use ExUnit.Case

  setup_all do
    {:ok, []}
  end

  test "executing a Cypher query, with invalid parameter value yields an error"  do
    cypher = """
      MATCH (n:Person {invalid: {an_elixir_datetime}}) RETURN TRUE
    """
    {:error, [code: :failure, message: message]} =
      Bolt.Sips.query(cypher, %{an_elixir_datetime: DateTime.utc_now})

    assert String.match?(message, ~r/unable to encode value: {\d+, \d+}/i)
  end
end
