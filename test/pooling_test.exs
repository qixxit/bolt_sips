defmodule Pooling.Test do
  use ExUnit.Case
  doctest Bolt.Sips

  setup_all do
    cypher_query = "CREATE (Patrick:Person {name:'Patrick Rothfuss', bolt_sips: true})"
    assert {:ok, _r} = Bolt.Sips.query(cypher_query)

    on_exit fn ->
      Bolt.Sips.query!("MATCH (p:Person) DETACH DELETE p")
      IO.puts "This is invoked once the test is done"
    end

    {:ok, []}
  end

  test "test that transaction is using only one pool worker" do
    pool_size = Bolt.Sips.config(:pool_size)
    max_overflow = Bolt.Sips.config(:max_overflow)
    limit = (pool_size + max_overflow) * 2
    pool_status_before = :poolboy.status(:bolt_sips_pool)

    # start transaction and run double the pool size queries
    tx_pid = Bolt.Sips.begin
    Enum.each(1..limit, fn (_) ->
      {:ok, _row} = Bolt.Sips.query(tx_pid, "MATCH (p:Person) RETURN p")
      assert  {:ready, 4, 0, 1} == :poolboy.status(:bolt_sips_pool)
    end)
    :ok = Bolt.Sips.commit(tx_pid)

    assert pool_status_before == :poolboy.status(:bolt_sips_pool)
  end

  test "test pool size limit" do
    pool_size = Bolt.Sips.config(:pool_size)
    max_overflow = Bolt.Sips.config(:max_overflow)
    limit = pool_size + max_overflow
    pool_status_before = :poolboy.status(:bolt_sips_pool)

    # start transaction and run double the pool size queries and never coomit
    # this results in filling up pool and block following transactions
    results = Enum.reduce(1..limit, {0, 0}, fn (_, {ready, overflow}) ->
      tx_pid = Bolt.Sips.begin
      {:ok, _row} = Bolt.Sips.query(tx_pid, "MATCH (p:Person) RETURN p")
      case elem(:poolboy.status(:bolt_sips_pool), 0) do
        :ready ->
          {ready + 1, overflow}
        _other ->
          {ready, overflow + 1}
      end
    end)

    assert {4, 2} == results
  end

#  test "test number of connections is withing defined pool size" do
#    pool_size = Bolt.Sips.config(:pool_size)
#    max_overflow = Bolt.Sips.config(:max_overflow)
#    limit = (pool_size + max_overflow) * 2
#
#    IO.puts "pool1: #{inspect :poolboy.status(:bolt_sips_pool)}"
#
#    tx_pid = Bolt.Sips.begin
#    {:ok, row} = Bolt.Sips.query(tx_pid, "MATCH (p:Person) RETURN p")
#    IO.puts "row: #{inspect row}"
#    IO.puts "pool2: #{inspect :poolboy.status(:bolt_sips_pool)}"
#    :ok = Bolt.Sips.commit(tx_pid)
#    IO.puts "pool3: #{inspect :poolboy.status(:bolt_sips_pool)}"

#    #foo = :supervisor.which_children({:local, :bolt_sips_pool})
#    #IO.puts "supervisor.childredn: #{inspect foo}"

#    open_conns = Enum.filter(Port.list, fn (p)  ->
#      case Port.info(p, :name) do
#        {:name, 'tcp_inet'} ->
#          {:connected, conn} = Port.info(p, :connected)
#          IO.puts "port.info.conn: #{inspect conn}"
#          ##{:ok, {{127, 0, 0, 1}, port_no}} = :inet.sockname(conn)
#          true
#        _ ->
#          false
#      end
#    end)
#    IO.puts "open_cons: #{inspect open_conns}"

    # get all unique open sockets to a database
    # open_conns = Enum.map(1..limit, fn (_) ->
    #   {:ok, _row} = Bolt.Sips.query("MATCH (p:Person) RETURN p")
    #   {:ok, {{127, 0, 0, 1}, port_no}} = :inet.sockname(conn)
    #   port_no
    # end)
    # unique_conns = Enum.reduce(open_conns, MapSet.new, fn(pid, acc) -> MapSet.put(acc, pid) end)
    # assert MapSet.size(unique_conns) <= pool_size + max_overflow

#    assert 1 != 2
#  end

end
