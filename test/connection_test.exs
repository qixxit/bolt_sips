defmodule Connection.Test do
  use ExUnit.Case
  doctest Bolt.Sips

  setup_all do
    {:ok, []}
  end

  test "test that open connection is closed after caller is killed" do
    [first_pid, second_pid] = Enum.map(1..2, fn (_) ->
      spawn(fn -> Bolt.Sips.conn end)
    end)

    assert Process.alive?(first_pid)
    assert Process.alive?(second_pid)

    Process.exit(first_pid, :kill)
    assert Process.alive?(first_pid) == false
    assert Process.alive?(second_pid)

    Process.exit(second_pid, :kill)
    assert Process.alive?(second_pid) == false
  end

  test "test explicit close of an open connections" do
    conn = Bolt.Sips.conn
    conn_pid = elem(Port.info(conn, :connected), 1)
    :ok = Bolt.Sips.close(conn)
    assert Process.alive?(conn_pid)
  end

end
