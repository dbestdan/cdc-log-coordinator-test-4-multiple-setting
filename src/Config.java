import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface Config {

	public final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	public final static Map<String, List<String>> clientTables = new HashMap<String, List<String>>() {
		{
			put("client1", new ArrayList<String>() {
				{
					add("bmsql_customer");
					add("bmsql_district");
					add("bmsql_history");
					add("bmsql_item");
					add("bmsql_new_order");
					add("bmsql_oorder");
					add("bmsql_stock");
				}
			});
			put("client2", new ArrayList<String>() {
				{
					add("bmsql_order_line");
				}
			});
		}
	};
	public final static List<String> table_group_1 = new ArrayList<String>() {
		{
			add("bmsql_customer");
			add("bmsql_warehouse");
			add("bmsql_district");
			add("bmsql_history");
			add("bmsql_item");
			add("bmsql_new_order");
			add("bmsql_oorder");			
		}
	};
	public final static List<String> table_group_2 = new ArrayList<String>() {
		{
			add("bmsql_order_line");
		}
	};
	public final static List<String> table_group_3 = new ArrayList<String>() {
		{
			add("bmsql_stock");
		}
	};

}
