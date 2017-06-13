defmodule Pooling.Test do
  use ExUnit.Case
  doctest Bolt.Sips

  setup_all do
    conn = Bolt.Sips.conn
    cypher_query = "CREATE (Patrick:Person {name:'Patrick Rothfuss', bolt_sips: true})"
    assert {:ok, _r} = Bolt.Sips.query(conn, cypher_query)

    on_exit fn ->
      Bolt.Sips.query!(conn, "MATCH (p:Person) DELETE p")
      IO.puts "This is invoked once the test is done"
    end

    {:ok, []}
  end

  test "test number of connections is withing defined pool size" do
    pool_size = Bolt.Sips.config(:pool_size)
    max_overflow = Bolt.Sips.config(:max_overflow)
    limit = (pool_size + max_overflow) * 2

    # get all unique open sockets to a database
    open_conns = Enum.map(1..limit, fn (_) ->
      conn = Bolt.Sips.conn
      {:ok, _row} = Bolt.Sips.query(conn, "MATCH (p:Person) RETURN p")
      {:ok, {{127, 0, 0, 1}, port_no}} = :inet.sockname(conn)
      port_no
    end)
    unique_conns = Enum.reduce(open_conns, MapSet.new, fn(pid, acc) -> MapSet.put(acc, pid) end)

    assert MapSet.size(unique_conns) <= pool_size + max_overflow
  end
end
