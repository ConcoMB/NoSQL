package nosql;

import nosql.adapter.Neo4jAdapter;

public class Main {

    public static void main (String args[]) {
    	 Neo4jAdapter adapter = new Neo4jAdapter();
         adapter.connect();
         adapter.insert();
         adapter.queryParams();
         if (args.length > 0) {
        	 String s = args[0];
        	 switch (Integer.valueOf(s)) {
        	 case 1:
        		 adapter.doQuery1();
        		 break;
        	 case 2:
        		 adapter.doQuery2();
        		 break;
        	 case 3:
        		 adapter.doQuery3();
        		 break;
        	 case 4:
        		 adapter.doQuery4();
        		 break;
        	 default:
        		 adapter.doQuery1();
        	 }
         } else {
    		 adapter.doQuery1();
         }
    }
}
