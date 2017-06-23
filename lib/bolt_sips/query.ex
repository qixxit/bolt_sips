defmodule Bolt.Sips.Query do
  @moduledoc """
  Provides a simple Query DSL.

  You can run simple Cypher queries with or w/o parameters, for example:

      {:ok, row} = Bolt.Sips.query(Bolt.Sips.conn, "match (n:Person {bolt_sips: true}) return n.name as Name limit 5")
      assert List.first(row)["Name"] == "Patrick Rothfuss"

  Or more complex ones:

      cypher = \"""
      MATCH (p:Person {bolt_sips: true})
      RETURN p, p.name AS name, upper(p.name) as NAME,
             coalesce(p.nickname,"n/a") AS nickname,
             { name: p.name, label:head(labels(p))} AS person
      \"""
      {:ok, r} = Bolt.Sips.query(conn, cypher)

  As you can see, you can organize your longer queries using the Elixir multiple line conventions, for readability.

  And there is one more trick, you can use for more complex Cypher commands: use `;` as a transactional separator.

  For example, say you want to clean up the test database **before** creating some tests entities. You can do that like this:

      cypher = \"""
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
      \"""
      assert {:ok, _r} = Bolt.Sips.query(conn, cypher)

  In the example above, this command: `MATCH (n {bolt_sips: true}) OPTIONAL MATCH (n)-[r]-() DELETE n,r;` will be executed in a distinct transaction, before all the other queries

  See the various tests, or more examples and implementation details.

  """

  @cypher_seps ~r/;(.){0,1}\n/

  def parse_statements(statements) when is_list(statements) do
    statements
  end
  def parse_statements(statements) do
    String.split(statements, @cypher_seps, trim: true)
    |> Enum.map(&(String.trim(&1)))
    |> Enum.filter(&(String.length(&1) > 0))
  end

  def begin(), do: "BEGIN"
  def commit(), do: "COMMIT"
  def rollback(), do: "ROLLBACK"

end
