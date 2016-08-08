package msk.mirbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.bridgedb.DataSource;
import org.bridgedb.IDMapperException;
import org.bridgedb.Xref;
import org.bridgedb.bio.DataSourceTxt;
import org.bridgedb.rdb.construct.DBConnector;
import org.bridgedb.rdb.construct.DataDerby;
import org.bridgedb.rdb.construct.GdbConstruct;
import org.bridgedb.rdb.construct.GdbConstructImpl3;

/**
 * Script to create a BridgeDb mapping database between
 * mature microRNA accession numbers and ids
 * @author msk
 *
 */
public class MatureIdMapper {
	
	// change bridgeName if needed = output file
	private static String bridgeName = "mirBase-mature-v21.bridge";
	private static String dbName = "mirBase mature";
	private static String dbVersion = "v21";
	private static String type = "mature microRNAs";
	
	private static String mirbaseUrl = "ftp://mirbase.org/pub/mirbase/CURRENT/aliases.txt.zip";
	
	public static void main(String[] args) {
		try {
			System.out.println("Read mirbase aliases...");
			Map<String, String> map = MatureIdMapper.retrieveAliases();
			
			System.out.println("Create new mapping database...");
			GdbConstruct db = MatureIdMapper.init();
			
			System.out.println("Create mappings...");
			MatureIdMapper.createMappings(map, db);
			
			System.out.println("Write database...");
			db.commit();
			db.finalize();
			
			System.out.println("Done...");
		} catch (MalformedURLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IDMapperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void createMappings(Map<String, String> map, GdbConstruct db) {
		for(String s : map.keySet()) {
			Xref main = new Xref(s, DataSource.getExistingBySystemCode("Mbm"));
			db.addGene(main);
			db.addLink(main, main);
			
			String [] buffer = map.get(s).split(";");
			for(String x : buffer) {
				Xref xref = new Xref(x, DataSource.getExistingBySystemCode("Mbm"));
				db.addGene(xref);
				db.addLink(main, xref);
			}
		}
	}
	
	private static Map<String, String> retrieveAliases() throws MalformedURLException, IOException {
		Map<String, String> map = new HashMap<String, String>();
		File tmp = new java.io.File("tmp");
		tmp.mkdir();
		Utils.unpackArchive(new URL(mirbaseUrl), tmp);
		File aliases = new File("tmp/aliases.txt");
		if(aliases.exists()) {
			BufferedReader reader = new BufferedReader(new FileReader(aliases));
			String line;
			while((line = reader.readLine()) != null) {
				if(line.startsWith("MIMAT")) {
					String [] buffer = line.split("\t");
					map.put(buffer[0], buffer[1]);
				}
			}
			reader.close();
		}
			
		// delete tmp directory
		for(File f : tmp.listFiles()) {
			f.delete();
		}
		tmp.delete();
		return map;
	}

	private static GdbConstruct init() throws IDMapperException {
		DataSourceTxt.init();
		
		if(new File(bridgeName).exists()) new File(bridgeName).delete();
		GdbConstruct newDb = GdbConstructImpl3.createInstance(bridgeName, new DataDerby(), DBConnector.PROP_RECREATE);
		newDb.createGdbTables();
		newDb.preInsert();
		newDb.setInfo("BUILDDATE", new SimpleDateFormat("yyyyMMdd").format(new Date()));
		newDb.setInfo("DATASOURCENAME", dbName);
		newDb.setInfo("DATASOURCEVERSION", dbVersion);
		newDb.setInfo("SERIES", type);
		newDb.setInfo("DATATYPE", type);
		return newDb;
	}
	
}
