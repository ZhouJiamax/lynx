package org.grapheco.cypher.syntax

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship}
import org.junit.jupiter.api.{Assertions, BeforeEach, Test}

import scala.collection.mutable.ArrayBuffer

class Parameters extends TestBase {

  val nodeInput = ArrayBuffer[(String, NodeInput)]()
  val relationshipInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name") -> LynxValue("Johan")))
  val n2 = TestNode(TestId(2), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name") -> LynxValue("Michael")))
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name") -> LynxValue("michael")))
  val n4 = TestNode(TestId(4), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name") -> LynxValue("Bob")))

  @BeforeEach
  def init(): Unit = {
    all_nodes.clear()
    all_rels.clear()
    nodeInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodeInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))
    nodeInput.append(("n3", NodeInput(n3.labels, n3.props.toSeq)))
    nodeInput.append(("n4", NodeInput(n4.labels, n4.props.toSeq)))

    model.write.createElements(nodeInput, relationshipInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      })

    model.write.commit
  }


  @Test
  def stringLiteral_1(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name = $name
        |RETURN n
        |""".stripMargin, Map(("name" -> "Johan"))).records().map(f => f("n").asInstanceOf[TestNode]).toArray
    Assertions.assertEquals(n1, records(0))
  }

  @Test
  def stringLiteral_2(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person { name: $name })
        |RETURN n
        |""".stripMargin, Map(("name" -> "Johan"))).records().map(f => f("n").asInstanceOf[TestNode]).toArray
    Assertions.assertEquals(n1, records(0))
  }

  @Test
  def regularExpression(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name =~ $regex
        |RETURN n.name
        |""".stripMargin, Map(("regex" -> ".*h.*"))).records().map(f => f("n.name").asInstanceOf[LynxValue].value).toArray
    Assertions.assertEquals(3, records.length)
    Assertions.assertEquals(Set("Johan", "Michael", "michael"), records.toSet)
  }

  @Test
  def caseSensitiveStringPatternMatching(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name STARTS WITH $name
        |RETURN n.name
        |""".stripMargin, Map(("name" -> "Michael"))).records().map(f => f("n.name").asInstanceOf[LynxValue].value).toArray
    Assertions.assertEquals(1, records.length)
    Assertions.assertEquals(Set("Michael"), records.toSet)
  }

  @Test
  def createNodeWithProperties(): Unit = {
    val num = nodeInput.length
    val _ = runOnDemoGraph(
      """
        |CREATE ($props)
        |""".stripMargin, Map("props" -> Map("name" -> "Andy", "position" -> "Developer")))
    /*after create node,check node whether not or exist*/
    val records = runOnDemoGraph(
      """
        |MATCH (n)
        |WHERE n.name='Andy'
        |RETURN n
        |""".stripMargin).records().map(f => f("n").asInstanceOf[TestNode]).toArray
    Assertions.assertEquals(num + 1, all_nodes.size)
    Assertions.assertEquals("Andy", records(0).props(LynxPropertyKey("name")).toString)
  }

  @Test
  def createMultipleNodesWithProperties(): Unit = {
    val num = nodeInput.length
    val records = runOnDemoGraph(
      """
        |UNWIND $props AS properties
        |CREATE (n:Person)
        |SET n = properties
        |RETURN n
        |""".stripMargin, Map("props" -> List(Map("awesome" -> "true", "name" -> "Andy", "position" -> "Developer"),
        Map("children" -> "3", "name" -> "Michael", "position" -> "Developer"))))
      .records().map(f => f("n").asInstanceOf[TestNode]).toArray

    Assertions.assertEquals(num + 2, all_nodes.size)
    Assertions.assertEquals(2, records.length)
    Assertions.assertEquals("Andy", records(0).props(LynxPropertyKey("name")).toString)
    Assertions.assertEquals("Michael", records(1).props(LynxPropertyKey("name")).toString)
  }

  @Test
  def settingAllPropertiesOnANode(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name='Michael'
        |SET n = $props
        |""".stripMargin,
      Map("props" -> Map("name" -> "Andy","position" -> "Developer")))
      .records().map(f => f("n").asInstanceOf[TestNode]).toArray

    Assertions.assertEquals("Andy", records(0).props(LynxPropertyKey("name")).toString)
    Assertions.assertEquals("Developer", records(0).props(LynxPropertyKey("position")).toString)
  }

  @Test
  def skipAndLimit(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |RETURN n.name
        |SKIP $s
        |LIMIT $l
        |""".stripMargin, Map("s" -> 1, "l" -> 1)).records().map(f => f("n.name").asInstanceOf[LynxValue].value).toArray
    Assertions.assertEquals(1, records.length)
    Assertions.assertEquals(Set("michael"), records.toSet)
  }

  @Test
  def nodeId(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n)
        |WHERE id(n)= $id
        |RETURN n.name
        |""".stripMargin, Map("id" -> 1)).records().map(f => f("n.name").asInstanceOf[LynxValue].value).toArray
    Assertions.assertEquals(1, records.length)
    Assertions.assertEquals(Set("Johan"), records.toSet)
  }

  @Test
  def multipleNodeIds(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n)
        |WHERE id(n) IN $ids
        |RETURN n.name
        |""".stripMargin, Map("ids" -> List(1, 2, 3))).records().map(f => f("n.name").asInstanceOf[LynxValue].value).toArray
    Assertions.assertEquals(3, records.length)
    Assertions.assertEquals(Set("Johan", "Michael", "michael"), records.toSet)
  }

}
