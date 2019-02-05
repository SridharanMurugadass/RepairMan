package com.connect.service.controller;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.WebRequest;
import com.connect.service.config.CassandraConfiguration;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

@RestController
public class RegisterController {

	@Autowired
	private CassandraConfiguration cassandraConfiguration;

	private Cluster cluster;
	private Session session;

	@PostConstruct
	public void cassandra() {
		cluster = Cluster.builder().addContactPoints(cassandraConfiguration.getContactPoints()).build();
		session = cluster.connect();
	}

	@RequestMapping(method = {
			RequestMethod.POST }, value = "/{keyspace}/{table1}/{table2}/{type}", consumes = "application/json")
	public ResponseEntity<String> register(@PathVariable String keyspace, @PathVariable String table1,
			@PathVariable String table2, @PathVariable int type, @RequestBody String data) {
		String msg = "";
		long count;
		switch (type) {

		case 1:
			count = checkExisting(keyspace, table1, data);
			if (count > 0) {
				msg = "Already Existing User";
			} else {
				session.execute(String.format("insert into %s.%s JSON '%s'", keyspace, table1, data));
				msg = "success";
			}
			break;
		case 2:
			count = checkExisting(keyspace, table2, data);
			if (count > 0) {
				msg = "Already Existing User";
			} else {
				session.execute(String.format("insert into %s.%s JSON '%s'", keyspace, table2, data));
				msg = "success";
			}
			break;
		case 3:
			count = checkExisting(keyspace, table1, data);
			if (count > 0) {
				msg = "Already Existing Customerr";
			} else {
				session.execute(String.format("insert into %s.%s JSON '%s'", keyspace, table1, data));
				msg = "success";
			}

			count = checkExisting(keyspace, table2, data);
			if (count > 0) {
				msg = "Already Existing Vendor";
			} else {
				session.execute(String.format("insert into %s.%s JSON '%s'", keyspace, table2, data));
				msg = "success";
			}
			break;
		default:
			break;
		}

		return ResponseEntity.ok(msg);
	}

	public long checkExisting(String keyspace, String table, String data) {

		JSONObject jsonObject = new JSONObject(data);
		long id = jsonObject.getLong("id");
		StringBuilder checkExisting = new StringBuilder("select count(*) as count from ").append(keyspace).append(".")
				.append(table).append(" where id=").append(id);

		Row row = session.execute(checkExisting.toString()).one();
		long count = row.getLong(0);

		return count;

	}

	@RequestMapping(method = {
			RequestMethod.POST }, value = "/{keyspace}/{table1}/{table2}/{id}/{type}", consumes = "application/json")
	public ResponseEntity<String> updateAddressDetails(@PathVariable String keyspace, @PathVariable String table1,
			@PathVariable String table2, @PathVariable long id, @PathVariable int type, @RequestBody String data) {
		JSONObject jsonObject = new JSONObject(data);

		long alternate_phno = 0;
		String address = jsonObject.getString("address");
		int zipcode = jsonObject.getInt("zipcode");
		alternate_phno = jsonObject.has("alternate_phone") ? jsonObject.getLong("alternate_phone") : 0;
		String city = jsonObject.getString("city");

		List<String> service_name = new ArrayList<String>();
		if (jsonObject.has("service_name")) {
			JSONArray vendorServiceIdArr = jsonObject.getJSONArray("service_name");

			for (int i = 0; i < vendorServiceIdArr.length(); i++) {
				service_name.add("'" + vendorServiceIdArr.getString(i) + "'");
			}

		}
		switch (type) {
		case 1:

			StringBuilder sbUpdate = new StringBuilder();
			sbUpdate.append("update ").append(keyspace).append(".").append(table1).append(" set address").append("='")
					.append(address).append("',city='").append(city).append("',zipcode=").append(zipcode)
					.append(",alternate_phone=").append(alternate_phno).append(" where id=").append(id);
			session.execute(sbUpdate.toString());
			break;

		case 2:

			StringBuilder sbUpdateVendor = new StringBuilder();

			sbUpdateVendor.append("update ").append(keyspace).append(".").append(table2).append(" set address")
					.append("='").append(address).append("',city='").append(city).append("',zipcode=").append(zipcode)
					.append(",alternate_phone=").append(alternate_phno).append(",service_name=").append(service_name)
					.append(" where id=").append(id);
			session.execute(sbUpdateVendor.toString());

			break;
		case 3:

			StringBuilder update = new StringBuilder();
			update.append("update ").append(keyspace).append(".").append(table1).append(" set address").append("='")
					.append(address).append("',city='").append(city).append("',zipcode=").append(zipcode)
					.append(",alternate_phone=").append(alternate_phno).append(" where id=").append(id);
			session.execute(update.toString());

			StringBuilder sbUpdate_Vendor = new StringBuilder();
			sbUpdate_Vendor.append("update ").append(keyspace).append(".").append(table2).append(" set address")
					.append("='").append(address).append("',city='").append(city).append("',zipcode=").append(zipcode)
					.append(",alternate_phone=").append(alternate_phno).append(",service_name=").append(service_name)
					.append("  where id=").append(id);
			session.execute(sbUpdate_Vendor.toString());

			break;
		default:
			break;
		}
		return ResponseEntity.ok("OK");
	}

	@RequestMapping(method = {
			RequestMethod.POST }, value = "/{keyspace}/{table}/{serviceName}", consumes = "application/json")
	public ResponseEntity<String> newServiceByVendor(@PathVariable String keyspace, @PathVariable String table,
			@PathVariable String serviceName) {
		long serviceCount = 0;
		String msg = "";
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		StringBuilder newService = new StringBuilder("select count(*) as serviceCount from ").append(keyspace)
				.append(".").append(table).append(" where service_name='").append(serviceName)
				.append("' allow filtering");
		Row row = session.execute(newService.toString()).one();
		serviceCount = row.getLong(0);
		if (serviceCount > 0) {
			msg = "Service already exist.Kindly select from the services listed above";
		} else {
			StringBuilder insertNewService = new StringBuilder("insert into ").append(keyspace).append(".")
					.append(table).append("(service_name,created_date,service_id)").append("values ('")
					.append(serviceName).append("','").append(timestamp).append("',").append("uuid()").append(")");
			session.execute(insertNewService.toString());

			msg = "New Service added";
		}
		return ResponseEntity.ok(msg);

	}

	public void incrementColumn(String keyspace, String table1, String custDetails, int custCount, long customerId,
			String table2, String vendorDetails, int vendorCount, long vendorId) {
		System.out.println("custDetails::" + custDetails + "vendorDetails " + vendorDetails + "custCount " + custCount
				+ "vendorCount " + vendorCount);
		StringBuilder custUpdate = new StringBuilder();
		StringBuilder vendorUpdate = new StringBuilder();

		custUpdate.append("update ").append(keyspace).append(".").append(table1).append(" set service_")
				.append(custCount).append("='").append(custDetails).append("',count=").append(custCount)
				.append(" where id=").append(customerId);

		vendorUpdate.append("update ").append(keyspace).append(".").append(table2).append(" set service_")
				.append(vendorCount).append("='").append(vendorDetails).append("',count=").append(vendorCount)
				.append(" where id=").append(vendorId);
		try {
			System.out.println("tryyyyyyyyyy ");

			System.out.println(custUpdate.toString());
			session.execute(custUpdate.toString());

			System.out.println(vendorUpdate.toString());
			session.execute(vendorUpdate.toString());
		} catch (Exception e) {

			System.out.println("catch " + e.toString());
			StringBuilder sb1 = new StringBuilder("alter table ").append(keyspace).append(".").append(table1)
					.append(" add service_").append(custCount).append(" text");
			System.out.println(sb1.toString());
			session.execute(sb1.toString());
			StringBuilder sb2 = new StringBuilder("alter table ").append(keyspace).append(".").append(table2)
					.append(" add service_").append(vendorCount).append(" text");
			System.out.println(sb2.toString());
			session.execute(sb2.toString());

			System.out.println("insideeee1  " + custUpdate.toString());
			session.execute(custUpdate.toString());
			System.out.println("insideeee2  " + vendorUpdate.toString());
			session.execute(vendorUpdate.toString());
		}

	}

	@RequestMapping(method = {
			RequestMethod.PUT }, value = "/{keyspace}/{table1}/{table2}/{customerId}/{vendorId}", consumes = "application/json")
	public ResponseEntity<String> service(@PathVariable String keyspace, @PathVariable String table1,

			@PathVariable String table2, @PathVariable long customerId, @PathVariable long vendorId) {

		JSONObject custJson = new JSONObject();
		JSONObject vendorJson = new JSONObject();
		int custCount, vendorCount;

		StringBuilder sbCustDetails = new StringBuilder("select id,name,count from ").append(keyspace).append(".")
				.append(table1).append(" where id=").append(customerId);
		Row row = session.execute(sbCustDetails.toString()).one();

		StringBuilder sbVendorDetails = new StringBuilder("select id,name,count from ").append(keyspace).append(".")
				.append(table2).append(" where id=").append(vendorId);
		Row row1 = session.execute(sbVendorDetails.toString()).one();

		// Adding vendorDetails to custList and customerDetails to vendorList

		custCount = row.getInt("count");
		custCount++;
		vendorCount = row1.getInt("count");
		vendorCount++;

		custJson.put("custSerCount", "service_" + vendorCount);
		custJson.put("vName", row1.getString("name"));
		custJson.put("vId", row1.getLong("id"));
		custJson.put("status", "Pending");

		vendorJson.put("vendorSerCount", "service_" + custCount);
		vendorJson.put("cName", row.getString("name"));
		vendorJson.put("cId", row.getLong("id"));
		vendorJson.put("status", "Pending");
		incrementColumn(keyspace, table1, custJson.toString(), custCount, customerId, table2, vendorJson.toString(),
				vendorCount, vendorId);
		return ResponseEntity.ok("OK");

	}

	@RequestMapping(method = {
			RequestMethod.PUT }, value = "/{keyspace}/{table1}/{table2}", consumes = "application/json")
	public ResponseEntity<String> changeStatus(@PathVariable String keyspace, @PathVariable String table1,
			@PathVariable String table2, @RequestBody String data) {

		JSONObject jsonObject = new JSONObject(data);
		String column1 = jsonObject.getString("column1");
		String column2 = jsonObject.getString("column2");
		String status = jsonObject.has("status") ? jsonObject.getString("status") : "";
		String feedback = jsonObject.has("feedback") ? jsonObject.getString("feedback") : "";
		int rating = jsonObject.has("rating") ? jsonObject.getInt("rating") : 0;
		long customerId = jsonObject.getLong("customerId");
		long vendorId = jsonObject.getLong("vendorId");

		StringBuilder sbUpdate = new StringBuilder();
		sbUpdate.append("update ").append(keyspace).append(".").append(table1).append(" set ").append(column1)
				.append("[3]='").append(status).append("' where id=").append(customerId);
		System.out.println("aaaa" + sbUpdate.toString());
		session.execute(sbUpdate.toString());

		StringBuilder sbUpdate1 = new StringBuilder();
		sbUpdate1.append("update ").append(keyspace).append(".").append(table2).append("  set ").append(column2)
				.append("[3]='").append(status).append("'");
		if (rating != 0)
			sbUpdate1.append(",").append(column2).append("=").append(column2).append("+['").append(rating).append("']");
		if (!"".equals(feedback))
			sbUpdate1.append(",").append(column2).append("=").append(column2).append("+['").append(feedback)
					.append("']");
		sbUpdate1.append(" where id=").append(vendorId);

		session.execute(sbUpdate1.toString());

		return ResponseEntity.ok("OK");
	}

	@RequestMapping(method = { RequestMethod.GET }, value = "/{keyspace}/{table}", produces = "application/json")
	public String get(@PathVariable String keyspace, @PathVariable String table) {

		StringBuilder sb = new StringBuilder("select JSON * from ").append(keyspace).append(".").append(table);

		String query = sb.toString();
		List<String> all = session.execute(query).all().stream().map(row -> row.getString("[json]"))
				.collect(Collectors.toList());
		return "[" + String.join(",", all) + "]";
	}

	@RequestMapping(method = { RequestMethod.GET }, value = "/{keyspace}/{table}/{id}", produces = "application/json")
	public String getById(@PathVariable String keyspace, @PathVariable String table, @PathVariable long id) {
		StringBuilder sb = new StringBuilder("select JSON * from ").append(keyspace).append(".").append(table)
				.append(" where id =").append(id);
		String query = sb.toString();
		List<String> all = session.execute(query).all().stream().map(row -> row.getString("[json]"))
				.collect(Collectors.toList());
		return "[" + String.join(",", all) + "]";
	}

	@RequestMapping(method = {
			RequestMethod.GET }, value = "{keyspace}/{table}/services", produces = "application/json")
	public String getServiceList(@PathVariable String keyspace, @PathVariable String table, WebRequest webRequest) {

		StringBuilder sb = new StringBuilder("select JSON * from ").append(keyspace).append(".").append(table);
		String serviceDetailsQuery = sb.toString();
		List<String> all = session.execute(serviceDetailsQuery).all().stream().map(row -> row.getString("[json]"))
				.collect(Collectors.toList());
		return "[" + String.join(",", all) + "]";

	}

	@RequestMapping(method = {
			RequestMethod.GET }, value = "getService/{keyspace}/{table}/{zipcode}/{service_name}", produces = "application/json")
	public String getService(@PathVariable String keyspace, @PathVariable String table, @PathVariable int zipcode,
			@PathVariable String service_name, WebRequest webRequest) {
		StringBuilder sb = new StringBuilder("select JSON id,address,city,count,name,email from ").append(keyspace)
				.append(".").append(table).append(" where zipcode=").append(zipcode)
				.append(" and service_name contains '").append(service_name).append("' allow filtering");
		String query = sb.toString();

		List<String> all = session.execute(query).all().stream().map(row -> row.getString("[json]"))
				.collect(Collectors.toList());
		return "[" + String.join(",", all) + "]";

	}

}
