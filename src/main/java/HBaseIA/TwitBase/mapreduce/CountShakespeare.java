package HBaseIA.TwitBase.mapreduce;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import HBaseIA.TwitBase.hbase.TwitsDAO;

public class CountShakespeare {

	public static class Map extends TableMapper<Text, LongWritable> {

		public static enum Counters {
			ROWS, SHAKESPEAREAN
		};

		private Random rand;

		/**
		 * Determines if the message pertains to Shakespeare.
		 */
		private boolean containsShakespear(String msg) {
			return rand.nextBoolean();
		}

		@Override
		protected void setup(Context context) {
			rand = new Random(System.currentTimeMillis());
		}

		@Override
		protected void map(ImmutableBytesWritable rowkey, Result result,
				Context context) {
			byte[] b = result.getColumnLatest(TwitsDAO.TWITS_FAM,
					TwitsDAO.TWIT_COL).getValue();
			if (b == null)
				return;

			String msg = Bytes.toString(b);
			if (msg.isEmpty())
				return;

			context.getCounter(Counters.ROWS).increment(1);
			if (containsShakespear(msg))
				context.getCounter(Counters.SHAKESPEAREAN).increment(1);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		System.out.println(conf.get("user.name"));

		java.util.Map<String, String> valByRegex = conf.getValByRegex("^");
		for (String key : valByRegex.keySet()) {
			System.out.println(key + " : " + valByRegex.get(key));
		}

		System.out.println("size :" + valByRegex.size());
		// conf.set("hbase.zookeeper.quorum", "192.168.25.37");
		conf.set("hbase.zookeeper.quorum", "jeff");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		// conf.set("hbase.master", "192.168.25.37:60000");
		conf.set("hbase.master", "jeff");
		conf.set("hadoop.job.ugi", "jeff");
		// conf.set("hbase.tmp.dir", "/tmp/hbase-jeff");
		// conf.set("hadoop.tmp.dir", "/tmp/hadoop-jeff");
		// conf.set("hbase.rootdir", "file:///tmp/hbase-jeff/hbase");
		// conf.set("hbase.zookeeper.property.clientPort", "12181");
		// conf.set("hbase.master.port", "41000");
		// conf.set("hbase.master.info.port", "41010");

		System.out.println(conf.get("hbase.zookeeper.property.clientPort"));
		System.out.println(conf.get("hbase.master.port"));
		System.out.println(conf.get("hbase.master.info.port"));
		System.out.println(conf.get("hbase.rootdir"));
		System.out.println(conf.get("hbase.tmp.dir"));
		System.out.println(conf.get("dfs.permissions.**superusergroup"));
		System.out.println(conf.get("hadoop.tmp.dir"));
		System.out.println(conf.get("mapreduce.framework.name"));
		System.out.println(conf.get("hadoop.job.ugi"));
		
		HBaseAdmin.checkHBaseAvailable(conf);
		
		
		HTablePool pool = new HTablePool(conf, Integer.MAX_VALUE);

		Job job = new Job(conf, "TwitBase Shakespeare counter");
		job.setJarByClass(CountShakespeare.class);

		Scan scan = new Scan();
		scan.addColumn(TwitsDAO.TWITS_FAM, TwitsDAO.TWIT_COL);
		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(TwitsDAO.TABLE_NAME), scan, Map.class,
				ImmutableBytesWritable.class, Result.class, job);

		job.setOutputFormatClass(NullOutputFormat.class);
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
