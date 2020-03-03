package com.sample.mr;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;


@SuppressWarnings("deprecation")
public class jsonlinereader
		extends RecordReader<LongWritable, Text> {

	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private LongWritable key = new LongWritable();
	private Text value = new Text();

	private static final Log LOG = LogFactory.getLog(
			jsonlinereader.class);

	@Override
	public void initialize(
			InputSplit genericSplit,
			TaskAttemptContext context)
			throws IOException {

		FileSplit split = (FileSplit) genericSplit;
		org.apache.hadoop.conf.Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt(
				"mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);


		start = split.getStart();
		end = start + split.getLength();

		final Path file = (Path) split.getPath();
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());

		boolean skipFirstLine = false;
		if (start != 0) {
			skipFirstLine = true;

			--start;
			fileIn.seek(start);
		}

		in = new LineReader(fileIn, job);


		if (skipFirstLine) {
			Text dummy = new Text();
			start += in.readLine(dummy, 0,
					(int) Math.min(
							(long) Integer.MAX_VALUE,
							end - start));
		}

		this.pos = start;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Text();
		}
		value.clear();
		final Text endline = new Text("\n");
		int newSize = 0;


		int max_count = 30;
		boolean keep_reading = true;
		loop: for(int i=0;i<max_count && keep_reading == true;i++){
			Text v = new Text();
			while (pos < end) {
				newSize = in.readLine(v, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));

				if  ( v.toString().trim().contains("}") ) {
					keep_reading = false;
				}

				if (newSize == 0) {
					break;
				}
				pos += newSize;
				if (newSize < maxLineLength) {
					break;
				}
			}

			String v_combined = "";

			if (!  ( v.toString().trim().contains("{") ||  v.toString().trim().contains("}")) ) {

				try {
					Pattern pattern = Pattern.compile("\"(\\w+)\"\\s?:\\s?\"?([\\w\\d\\s]+)\"?,?");
					Matcher matcher = pattern.matcher(v.toString().trim());
					matcher.find();

					v_combined += matcher.group(1).trim() + ":" + matcher.group(2).trim() + ",";
				} catch (Exception e) {
					System.out.println("CANNOT MATCH ");
					System.out.println(v.toString().trim());
				}

			} else continue loop;

			value.append(v_combined.getBytes(),0, v_combined.length());


		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}
}