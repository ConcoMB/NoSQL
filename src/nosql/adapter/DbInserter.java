package nosql.adapter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import nosql.adapter.Neo4jAdapter.RelTypes;
import nosql.utils.RandomUtils;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class DbInserter {

	final static int REGION_CARDINALITY = 5;
	final static int NATION_CARDINALITY = 25;
	final static int PART_CARDINALITY = 200;
	final static int SUPPLIER_CARDINALITY = 40;
	final static int CUSTOMER_CARDINALITY = 40;
	final static int PARTSUPP_CARDINALITY = 1000;
	final static int ORDER_CARDINALITY = 400;
	final static int LINEITEM_CARDINALITY = 200;

	final static int NUM_LINEITEMS = 200;
	final static double SF = (double) NUM_LINEITEMS
			/ (double) LINEITEM_CARDINALITY;
	final static int NUM_REGIONS = REGION_CARDINALITY;
	final static int NUM_NATIONS = NATION_CARDINALITY;
	final static int NUM_PARTS = (int) (PART_CARDINALITY * SF);
	final static int NUM_SUPPLIERS = (int) (SUPPLIER_CARDINALITY * SF);
	final static int NUM_CUSTOMERS = (int) (CUSTOMER_CARDINALITY * SF);
	final static int NUM_PARTSUPPS = (int) (PARTSUPP_CARDINALITY * SF);
	final static int NUM_ORDERS = (int) (ORDER_CARDINALITY * SF);

	final static int FIRST_REGION_NODE = 1;
	final static int FIRST_NATION_NODE = FIRST_REGION_NODE + NUM_REGIONS;
	final static int FIRST_PART_NODE = FIRST_NATION_NODE + NUM_NATIONS;
	final static int FIRST_SUPPLIER_NODE = FIRST_PART_NODE + NUM_PARTS;
	final static int FIRST_CUSTOMER_NODE = FIRST_SUPPLIER_NODE + NUM_SUPPLIERS;
	final static int FIRST_PARTSUPP_NODE = FIRST_CUSTOMER_NODE + NUM_CUSTOMERS;
	final static int FIRST_ORDER_NODE = FIRST_PARTSUPP_NODE + NUM_PARTSUPPS;

	private GraphDatabaseService database;

	private HashSet<ArrayList<Integer>> partSuppPK = new HashSet<ArrayList<Integer>>(
			NUM_PARTSUPPS);
	private HashSet<ArrayList<Integer>> lineItemPK = new HashSet<ArrayList<Integer>>(
			NUM_LINEITEMS);
	
	private static Random r = new Random(System.currentTimeMillis());

	public DbInserter(GraphDatabaseService database) {
		this.database = database;
	}

	public void insert() {
		Transaction tx = database.beginTx();
		try {
			insertRegions(NUM_REGIONS, 1);
			insertNations(NUM_NATIONS, 1, FIRST_REGION_NODE, NUM_REGIONS);
			insertParts(NUM_PARTS, 1);
			insertSuppliers(NUM_SUPPLIERS, 1, FIRST_NATION_NODE, NUM_NATIONS);
			insertCustomers(NUM_CUSTOMERS, 1, FIRST_NATION_NODE, NUM_NATIONS);
			insertPartsupps(NUM_PARTSUPPS, FIRST_PART_NODE, NUM_PARTS,
					FIRST_SUPPLIER_NODE, NUM_SUPPLIERS);
			insertOrders(NUM_ORDERS, 1, FIRST_CUSTOMER_NODE, NUM_CUSTOMERS);
			insertLineitems(NUM_LINEITEMS, FIRST_ORDER_NODE, NUM_ORDERS,
					FIRST_PARTSUPP_NODE, NUM_PARTSUPPS);
			tx.success();
			System.out.println("Insert complete");
		} finally {
			tx.finish();
		}
	}

	private void insertRegions(int numInserts, int firstInsertPK) {
		Label myLabel = DynamicLabel.label("Region");
		for (int i = 0; i < numInserts; i++) {
			Node node = database.createNode();
			node.setProperty("R_RegionKey", firstInsertPK + i);
			node.setProperty("R_Name", RandomUtils.randomString(32));
			node.setProperty("R_Comment", RandomUtils.randomString(80));
			node.setProperty("skip", RandomUtils.randomString(32));
			node.addLabel(myLabel);
		}
	}

	private void insertNations(int numInserts, int firstInsertPK,
			int firstRegionNode, int numRegions) {
		Label myLabel = DynamicLabel.label("Nation");
		for (int i = 0; i < numInserts; i++) {
			int regionNode = firstRegionNode + r.nextInt(numRegions);
			Node region = database.getNodeById(regionNode);
			Node node = database.createNode();
			node.setProperty("N_NationKey", firstInsertPK + i);
			node.setProperty("N_Name", RandomUtils.randomString(32));
			node.createRelationshipTo(region, RelTypes.MEMBER_OF_REGION);
			node.setProperty("N_Comment", RandomUtils.randomString(80));
			node.setProperty("skip", RandomUtils.randomString(32));
			node.addLabel(myLabel);
		}
	}

	private void insertParts(int numInserts, int firstInsertPK) {
		Label myLabel = DynamicLabel.label("Part");
		for (int i = 0; i < numInserts; i++) {
			Node node = database.createNode();
			node.setProperty("P_PartKey", firstInsertPK + i);
			node.setProperty("P_Name", RandomUtils.randomString(32));
			node.setProperty("P_Mfgr", RandomUtils.randomString(32));
			node.setProperty("P_Brand", RandomUtils.randomString(32));
			node.setProperty("P_Type", RandomUtils.randomString(32));
			node.setProperty("P_Size", RandomUtils.randomInt());
			node.setProperty("P_Container", RandomUtils.randomString(32));
			node.setProperty("P_RetailPrice", RandomUtils.randomDouble(13 / 2, 2));
			node.setProperty("P_Comment", RandomUtils.randomString(32));
			node.setProperty("skip", RandomUtils.randomString(32));
			node.addLabel(myLabel);
		}
	}

	private void insertSuppliers(int numInserts, int firstInsertPK,
			int firstNationNode, int numNations) {
		Label myLabel = DynamicLabel.label("Supplier");
		for (int i = 0; i < numInserts; i++) {
			int nationNode = firstNationNode + r.nextInt(numNations);
			Node nation = database.getNodeById(nationNode);
			Node node = database.createNode();
			node.setProperty("S_SuppKey", firstInsertPK + i);
			node.setProperty("S_Name", RandomUtils.randomString(32));
			node.setProperty("S_Address", RandomUtils.randomString(32));
			node.createRelationshipTo(nation, RelTypes.FROM_NATION);
			node.setProperty("S_Phone", RandomUtils.randomString(9));
			node.setProperty("S_AcctBal", RandomUtils.randomDouble(6, 2));
			node.setProperty("S_Comment", RandomUtils.randomString(52));
			node.setProperty("skip", RandomUtils.randomString(32));
			node.addLabel(myLabel);
		}
	}

	private void insertCustomers(int numInserts, int firstInsertPK,
			int firstNationNode, int numNations) {
		Label myLabel = DynamicLabel.label("Customer");
		for (int i = 0; i < numInserts; i++) {
			int nationNode = firstNationNode + r.nextInt(numNations);
			Node nation = database.getNodeById(nationNode);
			Node node = database.createNode();
			node.setProperty("C_CustKey", firstInsertPK + i);
			node.setProperty("C_Name", RandomUtils.randomString(32));
			node.setProperty("C_Address", RandomUtils.randomString(32));
			node.createRelationshipTo(nation, RelTypes.FROM_NATION);
			node.setProperty("C_Phone", RandomUtils.randomString(32));
			node.setProperty("C_AcctBal", RandomUtils.randomDouble(6, 2));
			node.setProperty("C_MktSegment", RandomUtils.randomString(32));
			node.setProperty("C_Comment", RandomUtils.randomString(60));
			node.setProperty("skip", RandomUtils.randomString(32));
			node.addLabel(myLabel);
		}
	}

	private void insertPartsupps(int numInserts, int firstPartNode,
			int numParts, int firstSupplierNode, int numSuppliers) {
		Label myLabel = DynamicLabel.label("Partsupp");
		for (int i = 0; i < numInserts; i++) {
			ArrayList<Integer> pk;
			int partNode, supplierNode;
			do {
				pk = new ArrayList<Integer>(2);
				partNode = firstPartNode + r.nextInt(numParts);
				supplierNode = firstSupplierNode + r.nextInt(numSuppliers);
				pk.add(partNode);
				pk.add(supplierNode);
			} while (partSuppPK.contains(pk));
			partSuppPK.add(pk);
			Node part = database.getNodeById(partNode);
			Node supplier = database.getNodeById(supplierNode);
			Node node = database.createNode();
			node.createRelationshipTo(part, RelTypes.IS_PART);
			node.createRelationshipTo(supplier, RelTypes.FROM_SUPPLIER);
			node.setProperty("PS_AvailQty", RandomUtils.randomInt());
			node.setProperty("PS_SupplyCost", RandomUtils.randomDouble(6, 2));
			node.setProperty("PS_Comment", RandomUtils.randomString(100));
			node.setProperty("skip", RandomUtils.randomString(32));
			node.addLabel(myLabel);
		}
	}

	private void insertOrders(int numInserts, int firstInsertPK,
			int firstCustomerNode, int numCustomers) {
		Label myLabel = DynamicLabel.label("Order");
		for (int i = 0; i < numInserts; i++) {
			int customerNode = firstCustomerNode + r.nextInt(numCustomers);
			Node customer = database.getNodeById(customerNode);

			Node node = database.createNode();
			node.setProperty("O_OrderKey", firstInsertPK + i);
			node.createRelationshipTo(customer, RelTypes.ORDERED_BY_CUSTOMER);
			node.setProperty("O_OrderStatus", RandomUtils.randomString(32));
			node.setProperty("O_TotalPrice", RandomUtils.randomDouble(6, 2));
			node.setProperty("O_OrderDate", RandomUtils.randomDate());
			node.setProperty("O_OrderPriority", RandomUtils.randomString(7));
			node.setProperty("O_Clerk", RandomUtils.randomString(32));
			node.setProperty("O_ShipPriority", RandomUtils.randomInt());
			node.setProperty("O_Comment", RandomUtils.randomString(40));
			node.setProperty("skip", RandomUtils.randomString(32));
			node.addLabel(myLabel);
		}
	}

	private void insertLineitems(int numInserts, int firstOrderNode,
			int numOrders, int firstPartsuppNode, int numPartsupps) {
		Label myLabel = DynamicLabel.label("Lineitem");
		for (int i = 0; i < numInserts; i++) {
			ArrayList<Integer> pk;
			int orderNode, linenumber;
			do {
				pk = new ArrayList<Integer>(2);
				orderNode = firstOrderNode + r.nextInt(numOrders);
				linenumber = RandomUtils.randomInt();
				pk.add(orderNode);
				pk.add(linenumber);
			} while (lineItemPK.contains(pk));
			lineItemPK.add(pk);
			Node order = database.getNodeById(orderNode);
			int partsuppNode = firstPartsuppNode + r.nextInt(numPartsupps);
			Node partsupp = database.getNodeById(partsuppNode);
			Node node = database.createNode();
			node.createRelationshipTo(order, RelTypes.MEMBER_OF_ORDER);
			node.createRelationshipTo(partsupp, RelTypes.HAS_PARTSUPP);
			node.setProperty("L_LineNumber", linenumber);
			node.setProperty("L_Quantity", RandomUtils.randomInt());
			node.setProperty("L_ExtendedPrice", RandomUtils.randomDouble(6, 2));
			node.setProperty("L_Discount", RandomUtils.randomDouble(6, 2));
			node.setProperty("L_Tax", RandomUtils.randomDouble(6, 2));
			node.setProperty("L_ReturnFlag", RandomUtils.randomString(32));
			node.setProperty("L_LineStatus", RandomUtils.randomString(32));
			node.setProperty("L_ShipDate", RandomUtils.randomDate());
			node.setProperty("L_CommitDate", RandomUtils.randomDate());
			node.setProperty("L_ReceiptDate", RandomUtils.randomDate());
			node.setProperty("L_ShipInstruct", RandomUtils.randomString(32));
			node.setProperty("L_ShipMode", RandomUtils.randomString(32));
			node.setProperty("L_Comment", RandomUtils.randomString(32));
			node.setProperty("skip", RandomUtils.randomString(32));
			node.addLabel(myLabel);
		}
	}
}
