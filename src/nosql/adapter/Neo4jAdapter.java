package nosql.adapter;

import java.util.Iterator;
import java.util.Random;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;

public class Neo4jAdapter {

	static enum RelTypes implements RelationshipType {
		MEMBER_OF_REGION, FROM_NATION, IS_PART, FROM_SUPPLIER, ORDERED_BY_CUSTOMER, MEMBER_OF_ORDER, HAS_PARTSUPP
	}

	private Long lineItemShipDate1;
	private Long lineItemShipDate2;
	private Integer partSize;
	private String partType;
	private String customerSegment;
	private String regionName1;
	private String regionName2;
	private Long orderDate1;
	private Long orderDate2;
	private Long orderDate3;

	private GraphDatabaseService database;
	private DbInserter dbInserter;
	private ExecutionEngine engine;
	private static Random r = new Random(System.currentTimeMillis());

	public void connect() {
		database = new GraphDatabaseFactory().newEmbeddedDatabase("neo4j");
		engine = new ExecutionEngine(database);
		dbInserter = new DbInserter(database);
	}

	public void insert() {
		dbInserter.insert();
	}

	public void disconnect() {
		database.shutdown();
	}

	public void doQuery1() {
		ExecutionResult result = engine
				.execute("START li=node(*) "
						+ "WHERE li.L_ShipDate <= "
						+ lineItemShipDate1
						+ " "
						+ "RETURN li.L_ReturnFlag, li.L_LineStatus, SUM(li.L_Quantity) AS Sum_Qty, SUM(li.L_ExtendedPrice) AS Sum_Base_Price, "
						+ "SUM(li.L_ExtendedPrice*(1 - li.L_Discount)) AS Sum_Disc_Price, SUM(li.L_ExtendedPrice*(1 - li.L_Discount)*(1 + li.L_Tax)) AS Sum_Charge, "
						+ "AVG(li.L_Quantity) AS Avg_Qty, AVG(li.L_ExtendedPrice) AS Avg_Price, AVG(li.L_Discount) AS Avg_Disc, COUNT(*) AS Count_Order "
						+ "ORDER BY li.L_ReturnFlag, li.L_LineStatus");
		System.out.println(result.dumpToString());
	}

	public void doQuery2() {
		long time = System.currentTimeMillis();
		ExecutionResult subResult = engine
				.execute("START r=node(*) "
						+ "MATCH (ps)-[:FROM_SUPPLIER]->(s)-[:FROM_NATION]->(n)-[:MEMBER_OF_REGION]->(r) "
						+ "WHERE r.R_Name = '" + regionName1 + "' "
						+ "RETURN MIN(ps.PS_SupplyCost) AS Min_Cost");
		System.out.println(System.currentTimeMillis() - time);
		double minCost = 0;
		Iterator<Double> minCostIter = subResult.columnAs("Min_Cost");
		for (Double cost : IteratorUtil.asIterable(minCostIter)) {
			minCost = cost;
		}

		ExecutionResult result = engine
				.execute("START ps=node(*) "
						+ "MATCH (p)<-[:IS_PART]-(ps)-[:FROM_SUPPLIER]->(s)-[:FROM_NATION]->(n)-[:MEMBER_OF_REGION]->(r) "
						+ "WHERE p.P_Size = "
						+ partSize
						+ " AND p.P_Type =~ '"
						+ partType
						+ "' AND r.R_Name = '"
						+ regionName1
						+ "' AND ps.PS_SupplyCost = "
						+ minCost
						+ " "
						+ "RETURN s.S_AcctBal, s.S_Name, n.N_Name, p.P_PartKey, p.P_Mfgr, s.S_Address, s.S_Phone, s.S_Comment "
						+ "ORDER BY s.S_AcctBal DESC, n.N_Name, s.S_Name, p.P_PartKey");
		System.out.println(result.dumpToString());
	}

	public void doQuery3() {
		ExecutionResult result = engine
				.execute("START li=node(*) "
						+ "MATCH (c)<-[:ORDERED_BY_CUSTOMER]-(o)<-[:MEMBER_OF_ORDER]-(li) "
						+ "WHERE c.C_MktSegment = '"
						+ customerSegment
						+ "' AND o.O_OrderDate < "
						+ orderDate1
						+ " AND li.L_ShipDate > "
						+ orderDate2
						+ " "
						+ "RETURN o.O_OrderKey, SUM(li.L_ExtendedPrice*(1 - li.L_Discount)) AS Revenue, o.O_OrderDate, o.O_ShipPriority "
						+ "ORDER BY Revenue DESC, o.O_OrderDate");
		System.out.println(result.dumpToString());
	}

	public void doQuery4() {
		ExecutionResult result = engine
				.execute("START ps=node(*) "
						+ "MATCH (c)<-[:ORDERED_BY_CUSTOMER]-(o)<-[:MEMBER_OF_ORDER]-(li)-[:HAS_PARTSUPP]->(ps)-[:FROM_SUPPLIER]->(s)-[:FROM_NATION]->(n)-[:MEMBER_OF_REGION]->(r) "
						+ "WHERE r.R_Name = '"
						+ regionName2
						+ "' AND o.O_OrderDate >= "
						+ orderDate2
						+ " AND o.O_OrderDate < "
						+ orderDate3
						+ " "
						+ "RETURN n.N_Name, SUM(li.L_ExtendedPrice*(1 - li.L_Discount)) AS Revenue "
						+ "ORDER BY Revenue DESC");
		System.out.println(result.dumpToString());
	}

	public void queryParams() {
		int lineItem = r.nextInt(DbInserter.NUM_LINEITEMS);
		ExecutionResult result = engine
				.execute("START li=node(*) WHERE HAS(li.L_ShipDate) RETURN li.L_ShipDate AS ShipDate SKIP "
						+ lineItem + " LIMIT 1");
		Iterator<Long> shipDateIter = result.columnAs("ShipDate");
		for (Long shipDate : IteratorUtil.asIterable(shipDateIter)) {
			lineItemShipDate1 = shipDate;
		}

		int part = r.nextInt(DbInserter.NUM_PARTS);
		result = engine
				.execute("START p=node(*) WHERE HAS(p.P_Size) RETURN p.P_Size AS Size SKIP "
						+ part + " LIMIT 1");
		Iterator<Integer> size_it = result.columnAs("Size");
		for (Integer size : IteratorUtil.asIterable(size_it)) {
			partSize = size;
		}
		part = r.nextInt(DbInserter.NUM_PARTS);
		result = engine
				.execute("START p=node(*) WHERE HAS(p.P_Type) RETURN p.P_Type AS Type SKIP "
						+ part + " LIMIT 1");
		Iterator<String> type_it = result.columnAs("Type");
		for (String type : IteratorUtil.asIterable(type_it)) {
			partType = type;
		}
		int region = r.nextInt(DbInserter.NUM_REGIONS);
		result = engine
				.execute("START r=node(*) WHERE HAS(r.R_Name) RETURN r.R_Name AS Name SKIP "
						+ region + " LIMIT 1");
		Iterator<String> nameIter = result.columnAs("Name");
		for (String name : IteratorUtil.asIterable(nameIter)) {
			regionName1 = name;
		}
		int order = r.nextInt(DbInserter.NUM_ORDERS);
		result = engine
				.execute("START o=node(*) WHERE HAS(o.O_OrderDate) RETURN o.O_OrderDate AS OrderDate SKIP "
						+ order + " LIMIT 1");
		Iterator<Long> orderDateIter = result.columnAs("OrderDate");
		for (Long orderDate : IteratorUtil.asIterable(orderDateIter)) {
			orderDate1 = orderDate;
		}
		lineItem = r.nextInt(DbInserter.NUM_LINEITEMS);
		result = engine
				.execute("START li=node(*) WHERE HAS(li.L_ShipDate) RETURN li.L_ShipDate AS ShipDate SKIP "
						+ lineItem + " LIMIT 1");
		shipDateIter = result.columnAs("ShipDate");
		for (Long shipDate : IteratorUtil.asIterable(shipDateIter)) {
			lineItemShipDate2 = shipDate;
		}
		int customer = r.nextInt(DbInserter.NUM_CUSTOMERS);
		result = engine
				.execute("START c=node(*) WHERE HAS(c.C_MktSegment) RETURN c.C_MktSegment AS MktSegment SKIP "
						+ customer + " LIMIT 1");
		Iterator<String> segmentIter = result.columnAs("MktSegment");
		for (String mktSegment : IteratorUtil.asIterable(segmentIter)) {
			customerSegment = mktSegment;
		}
		region = r.nextInt(DbInserter.NUM_REGIONS);
		result = engine
				.execute("START r=node(*) WHERE HAS(r.R_Name) RETURN r.R_Name AS Name SKIP "
						+ region + " LIMIT 1");
		nameIter = result.columnAs("Name");
		for (String name : IteratorUtil.asIterable(nameIter)) {
			regionName2 = name;
		}
		order = r.nextInt(DbInserter.NUM_ORDERS);
		result = engine
				.execute("START o=node(*) WHERE HAS(o.O_OrderDate) RETURN o.O_OrderDate AS OrderDate SKIP "
						+ order + " LIMIT 2");
		orderDateIter = result.columnAs("OrderDate");
		int i = 0;
		for (Long orderDate : IteratorUtil.asIterable(orderDateIter)) {
			if (i == 0) {
				orderDate2 = orderDate;
			} else {
				orderDate3 = orderDate;
			}
			i++;
		}
		if (orderDate2 > orderDate3) {
			Long auxDate = orderDate2;
			orderDate2 = orderDate3;
			orderDate3 = auxDate;
		}
	}

}