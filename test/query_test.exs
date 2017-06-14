defmodule Query.Test do
  use ExUnit.Case, async: true
  alias Query.Test

  defmodule TestUser do
    defstruct name: "", bolt_sips: true
  end

  setup_all do
    # reuse the same connection for all the tests in the suite
    # MATCH (n {bolt_sips: TRUE}) OPTIONAL MATCH (n)-[r]-() DELETE n,r;

    # assert {:ok, _r} = Bolt.Sips.query("MATCH (n:Friend {star_friend: true}) OPTIONAL MATCH (n)-[r]-() DELETE n,r")

    cypher = """
    MATCH (n {bolt_sips: true}) OPTIONAL MATCH (n)-[r]-() DELETE n,r;

    CREATE (BoltSips:BoltSips {title:'Elixir sipping from Neo4j, using Bolt', released:2016, license:'MIT', bolt_sips: true})
    CREATE (TNOTW:Book {title:'The Name of the Wind', released:2007, genre:'fantasy', bolt_sips: true})
    CREATE (Patrick:Person {name:'Patrick Rothfuss', bolt_sips: true})
    CREATE (Kvothe:Person {name:'Kote', bolt_sips: true})
    CREATE (Denna:Person {name:'Denna', bolt_sips: true})
    CREATE (Chandrian:Deamon {name:'Chandrian', bolt_sips: true})

    CREATE
      (Kvothe)-[:ACTED_IN {roles:['sword fighter', 'magician', 'musician']}]->(TNOTW),
      (Denna)-[:ACTED_IN {roles:['many talents']}]->(TNOTW),
      (Chandrian)-[:ACTED_IN {roles:['killer']}]->(TNOTW),
      (Patrick)-[:WROTE]->(TNOTW)
    """
    assert {:ok, _r} = Bolt.Sips.query(cypher)

    # on_exit fn ->
    #   Bolt.Sips.query!("MATCH (n:Friend {star_friend: true}) OPTIONAL MATCH (n)-[r]-() DELETE n,r")
    #   IO.puts "This is invoked once the test is done"
    # end

    {:ok, []}
  end

  test "a simple query that should work" do
    cyp = """
      MATCH (n:Person {bolt_sips: true})
      RETURN n.name AS Name
      ORDER BY Name DESC
      LIMIT 5
    """
    {:ok, row} = Bolt.Sips.query(cyp)
    assert List.first(row)["Name"] == "Patrick Rothfuss",
           "missing 'The Name of the Wind' database, or data incomplete"
  end

  test "executing a Cypher query, with parameters" do
    cypher = """
      MATCH (n:Person {bolt_sips: true})
      WHERE n.name = {name}
      RETURN n.name AS name
    """
    case Bolt.Sips.query(cypher, %{name: "Kote"}) do
      {:ok, rows} ->
        refute length(rows) == 0, "Did you initialize the 'The Name of the Wind' database?"
        refute length(rows) > 1, "Kote?! There is only one!"
        assert List.first(rows)["name"] == "Kote", "expecting to find Kote"
      {:error, reason} -> IO.puts "Error: #{reason["message"]}"
    end
  end

  test "executing a Cpyher query, with struct parameters" do
    cypher = """
      CREATE(n:User {props})
    """

    assert {:ok, _} = Bolt.Sips.query(cypher, %{props: %Test.TestUser{name: "Strut", bolt_sips: true}})
  end

  test "executing a Cpyher query, with map parameters" do
    cypher = """
      CREATE(n:User {props})
    """

    assert {:ok, _} = Bolt.Sips.query(cypher, %{props: %{name: "Mep", bolt_sips: true}})
  end

  test "executing a raw Cypher query with alias, and no parameters" do
    cypher = """
      MATCH (p:Person {bolt_sips: true})
      RETURN p, p.name AS name, upper(p.name) as NAME,
             coalesce(p.nickname,"n/a") AS nickname,
             { name: p.name, label:head(labels(p))} AS person
      ORDER BY name DESC
    """
    {:ok, r} = Bolt.Sips.query(cypher)

    assert length(r) == 3, "you're missing some characters from the 'The Name of the Wind' db"

    if row = List.first(r) do
      assert row["p"].properties["name"] == "Patrick Rothfuss"
      assert is_map(row["p"]), "was expecting a map `p`"
      assert row["person"]["label"] == "Person"
      assert row["NAME"] == "PATRICK ROTHFUSS"
      assert row["nickname"] == "n/a"
      assert row["p"].properties["bolt_sips"] == true
    else
      IO.puts "Did you initialize the 'The Name of the Wind' database?"
    end
  end

  test "if Patrick Rothfuss wrote The Name of the Wind" do
    cypher = """
      MATCH (p:Person)-[r:WROTE]->(b:Book {title: 'The Name of the Wind'})
      RETURN p
    """

    rows = Bolt.Sips.query!(cypher)
    assert List.first(rows)["p"].properties["name"] == "Patrick Rothfuss"
  end

  test "it returns only known role names" do
    cypher = """
      MATCH (p)-[r:ACTED_IN]->() where p.bolt_sips RETURN r.roles as roles
      LIMIT 25
    """
    rows = Bolt.Sips.query!(cypher)
    roles = ["killer", "sword fighter","magician","musician","many talents"]
    my_roles = Enum.map(rows, &(&1["roles"])) |> List.flatten
    assert my_roles -- roles == [], "found more roles in the db than expected"
  end

  test "path from: MERGE p=({name:'Alice'})-[:KNOWS]-> ..." do
    cypher = """
    MERGE p = ({name:'Alice', bolt_sips: true})-[:KNOWS]->({name:'Bob', bolt_sips: true})
    RETURN p
    """
    path = Bolt.Sips.query!(cypher)
    |> List.first
    |> Map.get("p")

    assert {2,1} == {length(path.nodes), length(path.relationships)}
  end

  test "return a single number from a statement with params" do
    row = Bolt.Sips.query!("RETURN {n} AS num", %{n: 10}) |> List.first
    assert row["num"] == 10
  end

  test "run simple statement with complex params" do
    row = Bolt.Sips.query!("RETURN {x} AS n", %{"x": %{"abc": ["d", "e", "f"]}}) |> List.first
    assert row["n"]["abc"] == ["d", "e", "f"]
  end

  test "return an array of numbers" do
    row = Bolt.Sips.query!("RETURN [10,11,21] AS arr") |> List.first
    assert row["arr"] == [10, 11, 21]
  end

  test "return a string" do
    row = Bolt.Sips.query!("RETURN 'Hello' AS salute") |> List.first
    assert row["salute"] == "Hello"
  end

  test "UNWIND range(1, 10) AS n RETURN n" do
    rows = Bolt.Sips.query!("UNWIND range(1, 10) AS n RETURN n")
    assert {1, 10} == rows |> Enum.map(&(&1["n"])) |> Enum.min_max
  end

  test "MERGE (k:Person {name:'Kote'}) RETURN k" do
    k = Bolt.Sips.query!("MERGE (k:Person {name:'Kote', bolt_sips: true}) RETURN k LIMIT 1") |> List.first
    |> Map.get("k")

    assert k.labels == ["Person"]
    assert k.properties["name"] == "Kote"
  end

  test "query/2 and query!/2" do
    r = Bolt.Sips.query!("RETURN [10,11,21] AS arr") |> List.first
    assert r["arr"] == [10,11,21]

    assert{:ok, [r]} == Bolt.Sips.query("RETURN [10,11,21] AS arr")
    assert r["arr"] == [10,11,21]
  end

  test "create a Bob node and check it was deleted afterwards" do
    %{stats: stats} = Bolt.Sips.query!("CREATE (a:Person {name:'Bob'})")
    assert stats == %{"labels-added" => 1, "nodes-created" => 1, "properties-set" => 1}

    bob =
      Bolt.Sips.query!("MATCH (a:Person {name: 'Bob'}) RETURN a.name AS name")
      |> Enum.map(&(&1["name"]))

    assert bob == ["Bob"]

    %{stats: stats} = Bolt.Sips.query!("MATCH (a:Person {name:'Bob'}) DELETE a")
    assert stats["nodes-deleted"] == 1
  end

  test "Cypher version 3" do
    r = Bolt.Sips.query!("EXPLAIN RETURN 1") |> List.first
    refute r.plan == nil
    assert Regex.match?(~r/CYPHER 3/iu, r.plan["args"]["version"])
  end

  test "EXPLAIN MATCH (n), (m) RETURN n, m" do
    r = Bolt.Sips.query!("EXPLAIN MATCH (n), (m) RETURN n, m") |> List.first
    refute r.notifications == nil
    refute r.plan == nil
    assert List.first(r.plan["children"])["operatorType"] == "CartesianProduct"
  end

  test "can execute a query after a failure" do
    assert {:error, _} = Bolt.Sips.query("INVALID CYPHER")
    assert {:ok, [%{"n" => 22}]} = Bolt.Sips.query("RETURN 22 as n")
  end
end
