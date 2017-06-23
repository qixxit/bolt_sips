defmodule Rety.Backoff.Test do
  use ExUnit.Case, async: true
  import Stream

  use Retry

  setup_all do
    {:ok, []}
  end

  test "retry retries execution for specified attempts using an invalid Cypher command" do
    {elapsed, _} = :timer.tc fn ->
      result = retry with: lin_backoff(500, 1) |> take(5) do
        Bolt.Sips.query("INVALID CYPHER")
      end

      assert result == {:error, [code: "Neo.ClientError.Statement.SyntaxError",
              message: "Invalid input 'I': expected <init> (line 1, column 1 (offset: 0))\n\"INVALID CYPHER\"\n ^"]}
    end

    assert elapsed/1000 >= 2500
  end
end
