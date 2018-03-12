package com.orco.test;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Test {

	/**
	 * putIfAbsent
	 * 如果（调用该方法时）key-value 已经存在，则返回那个 value 值。
	 * 如果调用时 map 里没有找到 key 的 mapping，返回一个 null 值
	 */
	@org.junit.Test
	public void testConcurrentMap(){
		ConcurrentMap map = new ConcurrentHashMap();
		map.put(1,1);
		System.out.println(map.putIfAbsent(2,2));
		System.out.println(map.toString());
	}

	@org.junit.Test
	public void testSocket(){
		final long preResolveHost = System.nanoTime();
		final InetSocketAddress resolvedAddress = new InetSocketAddress("116.62.190.105", 5432);
		final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
		System.out.println(System.nanoTime());
		System.out.println(hostResolveTimeMs);
		if (hostResolveTimeMs > 2000) {
			System.out.println(resolvedAddress+":"+hostResolveTimeMs);
		} else {
			System.err.println(resolvedAddress+":"+hostResolveTimeMs);
		}
	}

	@org.junit.Test
	public void testAssert(){
		//断言1结果为true，则继续往下执行
		assert true;
		System.out.println("断言1没有问题，Go！");

		System.out.println("\n-----------------\n");

		//断言2结果为false,程序终止
		assert false : "断言失败，此表达式的信息将会在抛出异常的时候输出！";
		System.out.println("断言2没有问题，Go！");
	}
}
