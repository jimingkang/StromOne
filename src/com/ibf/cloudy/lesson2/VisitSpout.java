package com.ibf.cloudy.lesson2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class VisitSpout implements IRichSpout{

	//璇ョ被鑳芥妸鏁版嵁锛屼紶鍒颁笅涓�骇缁勪欢
	SpoutOutputCollector collector = null;
	BufferedReader br =  null;
	
	@Override
	public void ack(Object arg0) {
		// 褰揃olt绫婚噷鎵ц鎴愬姛锛屽垯鍥炶皟璇ュ嚱鏁�
		
	}

	@Override
	public void activate() {
		// 婵�椿Topo
		
	}

	@Override
	public void close() {
		// 鍏抽棴Topo
		
	}

	@Override
	public void deactivate() {
		// 闈炴縺娲�
		
	}

	@Override
	public void fail(Object arg0) {
		// 褰揃olt绫婚噷鎵ц澶辫触锛屽垯鍥炶皟璇ュ嚱鏁�
		
	}

	@Override
	public void nextTuple() {
		// 浠庡閮ㄦ暟鎹簮鎷挎暟鎹�
		// 姝诲惊鐜�
		/**
		 * 璇绘枃浠讹紝鏂囦欢浣滀负鎴戜滑鐨勭粌涔犳暟鎹簮
		 */
		try {
			// date, provinceid ,url
			
			String data = null;
		
			while((data = br.readLine())!=null)
			{
//			   System.out.println(data); 
			   String arr[] = data.split("\t");
			   if (arr.length>25) {
				   System.out.println("date:"+arr[17]);
				   System.out.println("provinceId:"+arr[23]);
				 String date = arr[17] ; // 2015-08-28
				 String provinceId = arr[23] ;
				 String url = arr[1];
				 
				 if(date!=null && provinceId!=null && url!=null)
				 {
					 collector.emit(new Values(date,provinceId,url));
				 }
			   }
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		// 鍒濆鍖栧嚱鏁�
		this.collector = collector ;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream("E:/pv_UV/2015082818")));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 瀹氫箟杈撳嚭鏁版嵁鐨勫瓧娈靛悕
		declarer.declare(new Fields("date","provinceId","url"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// 閰嶇疆鐜鍙橀噺
		return null;
	}
	

}
