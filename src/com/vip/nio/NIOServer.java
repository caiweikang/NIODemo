package com.vip.nio;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class NIOServer {
	public static void main(String[] args) throws Exception {
		// 1.创建选择器，通常全局唯一
		Selector selc = Selector.open();
		
		// 2.创建通道
		ServerSocketChannel ssc = ServerSocketChannel.open();
		ssc.configureBlocking(false);
		// 3.指定监听的端口
		ssc.socket().bind(new InetSocketAddress(9999));

		// 4.将通道注册到selc上关注ACCEPT事件
		ssc.register(selc, SelectionKey.OP_ACCEPT);

		//--开始循环进行select操作，处理就绪的sk
		while(true){
			// 5.判断注册的通道是否已准备就绪
			int selcCount = selc.select();
			
			if(selcCount > 0){
				//--选择出已经就绪的sk
				Set<SelectionKey> set = selc.selectedKeys();
				//--遍历处理sk对应的通道的事件
				Iterator<SelectionKey> it = set.iterator();
				while(it.hasNext()){
						//--遍历出每一个就绪的sk
						SelectionKey sk = it.next();
						//--根据sk注册的不同，分别处理
						if(sk.isAcceptable()){//如果是一个ACCEPT操作
							//--获取sk对应的channel
							ServerSocketChannel sscx = (ServerSocketChannel) sk.channel();
							//--接受连接，得到sc
							SocketChannel sc = sscx.accept();
							//--开启sc的非阻塞模式
							sc.configureBlocking(false);
							//--将sc注册到selc上，关注READ方法
							sc.register(selc, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
						}else if(sk.isReadable()){//如果是一个Read操作
							//--获取sk对应的通道
							SocketChannel sc = (SocketChannel) sk.channel();
							
							//--获取头信息，获知体的长度
							ByteBuffer temp = ByteBuffer.allocate(1);
							String head = "";
							while(!head.endsWith("\r\n")){
								sc.read(temp);
								head += new String(temp.array());
								temp.clear();
							}
							int len = Integer.parseInt(head.substring(0,head.length()-2));
							
							//准备缓冲区接受数据
							ByteBuffer buf = ByteBuffer.allocate(len);
							while(buf.hasRemaining()){
								sc.read(buf);
							}
							
							//打印数据
							String msg = new String(buf.array(),"utf-8");
							System.out.println("服务器收到了客户端["+sc.socket().getInetAddress().getHostAddress()+"]发来的数据："+msg);
						}else if(sk.isWritable()){//如果是一个Write操作
							//--获取通道
							SocketChannel scx = (SocketChannel) sk.channel();
							
							//--待发送的数据
							String str = "是的，但我已满身伤痕，只是跟你没疼痛！";
							//--处理协议
							String sendStr = str.getBytes("utf-8").length +"\r\n" + str;
							
							//--发送数据
							ByteBuffer buf = ByteBuffer.wrap(sendStr.getBytes("utf-8"));
							while(buf.hasRemaining()){
								scx.write(buf);
							}
							//--取消WRITE注册
							scx.register(selc, sk.interestOps() & ~SelectionKey.OP_WRITE);
							
						}else{//其他就报错
							throw new RuntimeException("NIO操作方式出错！");
						}
					//--从已选择键集中删除已处理过后的键
					it.remove();
				}
				
			}
		}
	}
}
