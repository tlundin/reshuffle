package com.teraim.reshuffle;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import com.teraim.fieldapp.synchronization.SyncEntry;

public class Reshuffle {

	public static void main(String[] args) {

		Reshuffle re = new Reshuffle();
		
		
		try {
			Connection con = re.connectDatabase();
			
			PreparedStatement in_audit_stmt = con.prepareStatement("INSERT INTO [dbo].[audit_c] "
					+ "( [SYNCGROUP]"
					+ ",[USER]"
					+ ",[APP]"
					+ ",[TIMEOFINSERT]"
					+ ",[SYNCOBJECTS] )"
					+ " VALUES (?,?,?,?,?)");
	
			
			Statement stmt = con.createStatement();
			ResultSet r = stmt.executeQuery("SELECT * FROM [dbo].[convert] ORDER BY ORIG_ID");
			int id = -1;
			boolean first = true;
			String syncGroup=null,user=null,app=null;
			Timestamp toi = null;
			List<SyncEntry>m = new ArrayList<SyncEntry>();
			while (r.next()) {	
				int orig_id = r.getInt("ORIG_ID");
				if (id != -1 && id!=orig_id) {
					//write out array when id changes.
					SyncEntry[] seA = new SyncEntry[m.size()];
					seA = m.toArray(seA);
					byte[] bytes = re.objToByte(seA);
					Blob blob = new javax.sql.rowset.serial.SerialBlob(bytes);
					first = true;
					//write to DB.
					int p=1;
					in_audit_stmt.setString(p++,syncGroup);
					in_audit_stmt.setString(p++,user);
					in_audit_stmt.setString(p++,app);
					in_audit_stmt.setTimestamp(p++,toi);
					in_audit_stmt.setBlob(p++, blob);
					in_audit_stmt.executeUpdate();
					p("CurrID: "+id+"NextID: "+orig_id+" SyG: "+syncGroup+" u: "+user+" app: "+app+" ts: "+toi+" size: "+m.size());
					m.clear();
				}
				SyncEntry se = new SyncEntry(SyncEntry.action(r.getString("TYPE")),r.getString("CHANGES"),
						r.getLong("TIMEINENTRY"),r.getString("TARGET"),r.getString("AUTHOR"));
				m.add(se);
				if (first) {
					first = false;
				syncGroup = r.getString("SYNCGROUP");
				user = r.getString("USER");
				app = r.getString("APP");
				toi = r.getTimestamp("TIMEOFINSERT");
				id = orig_id;
				}
				
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	private Connection connectDatabase() {
		final String connectionUrl = "jdbc:sqlserver://ebdb.cljwr0n66av2.eu-west-1.rds.amazonaws.com;user=kalle;password=AbraKadabra!1;databaseName=Rlo_prod;";
		Connection con=null;
		try {

			con = DriverManager.getConnection(connectionUrl);
			if (con==null)
				System.err.println("Connection to database failed.");


		} catch (SQLException e) {

			e.printStackTrace();

		}
		return con;
	}

	static void p(String s) {
		System.out.println(s);
	}

	private byte[] objToByte(Object object) { 
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		byte[] bytes;
		try {
			out = new ObjectOutputStream(bos);   
			out.writeObject(object);
			bytes = bos.toByteArray();

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException ex) {
				// ignore close exception
			}
			try {
				bos.close();
			} catch (IOException ex) {
				// ignore close exception
			}
		}
		return bytes;
	}

}
