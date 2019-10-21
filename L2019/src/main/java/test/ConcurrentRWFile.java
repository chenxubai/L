package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

public class ConcurrentRWFile {
	private static String dir ;
	private static int fileSize;
	private static ExecutorService es;
	private static int corePoolSize;
	private static Random r = new Random(System.currentTimeMillis());
	private static int splitSize = 1024*1024*10;

	public static void main(String[] args) throws Exception {
		InputStream is = System.in;
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		System.out.print("please input dir:");
		dir = br.readLine();
		System.out.print("please input fileSize[MB]:");
		fileSize = 1024*1024* Integer.parseInt(br.readLine());
		System.out.print("please input corePoolSize:");
		corePoolSize = Integer.parseInt(br.readLine());
		System.out.print("please input times:");
		int times = Integer.parseInt(br.readLine());
		br.close();
		
		for(int i=0; i<times; i++) {
			System.out.println("rwFile time ["+i+"] ");
			File randomFile = randomFile("rwFile",i+"");
//			Long crc321 = crc32(randomFile);
			List<File> splits = rwSplitFile(randomFile, false);
//			File compactFile = compactFile(splits);
//			Long crc322 = crc32(compactFile);
//			if(crc321.longValue() != crc322.longValue()) {
//				throw new Exception("crc321:"+crc321+" ,crc322:"+crc322);
//			}
			randomFile.delete();
//			compactFile.delete();
		}
		es = new ScheduledThreadPoolExecutor(corePoolSize);
		for(int i=0; i<times; i++) {
			System.out.println("concurrent time ["+i+"] ");
			File randomFile = randomFile("concurrent",i+"");
//			Long crc321 = crc32(randomFile);
			List<File> rwFileConcurrent = rwFileConcurrent(randomFile,false);
//			File compactFile = compactFile(rwFileConcurrent);
//			Long crc322 = crc32(compactFile);
//			if(crc321.longValue() != crc322.longValue()) {
//				throw new Exception("crc321:"+crc321+" ,crc322:"+crc322);
//			}
			randomFile.delete();
//			compactFile.delete();
		}
		es.shutdown();
	}
	
	private static File randomFile(String prefix, String suffix) throws IOException {
		long start = System.currentTimeMillis();
		File file = new File(dir,prefix+"-random-"+suffix);
		FileOutputStream fos = new FileOutputStream(file);
		
		byte[] bytes = new byte[1024];
		for(int i=0; i<fileSize/1024; i++) {
			r.nextBytes(bytes);
			fos.write(bytes);
			fos.flush();
		}
		fos.close();
		System.out.println("	random file["+file.getName()+"] size["+fileSize+"] use time ["+(System.currentTimeMillis()-start)+"] ");
		return file;
	}
	 
    private static Long crc32(File file) throws IOException {
    	long start = System.currentTimeMillis();
        CRC32 crc32 = new CRC32();
        FileInputStream fileinputstream = new FileInputStream(file);
        CheckedInputStream checkedinputstream = new CheckedInputStream(fileinputstream, crc32);
        while (checkedinputstream.read() != -1) {
        }
        checkedinputstream.close();
        System.out.println("crc32 use time ["+(System.currentTimeMillis()-start)+"] ");
        return crc32.getValue();
    }
	
	private static List<File> rwSplitFile(File f, boolean deleteSplitedFiles) throws IOException {

		long start = System.currentTimeMillis();
		FileInputStream fis = new FileInputStream(f);
		FileChannel fisChannel = fis.getChannel();
		List<File> splitedFiles = new ArrayList<File>();
		
		long position = 0; 
		while(position<f.length()) {
			File splitedFile = new File(dir,"rwFile-"+position);
			splitedFiles.add(splitedFile);
			FileOutputStream fos = new FileOutputStream(splitedFile);
			FileChannel fosChannel = fos.getChannel();
			long transfered = fisChannel.transferTo(position, splitSize, fosChannel);
			
			fos.flush();
			fos.close();
			if(deleteSplitedFiles)
				splitedFile.delete();
			position = position + transfered;
		}
		
		fis.close();
		long save = System.currentTimeMillis();
		System.out.println("	fileSize "+fileSize+" RW use time "+(save-start));
		return splitedFiles;
	}
	
	private static File compactFile(List<File> splits) throws IOException {
		long start = System.currentTimeMillis();
		File compactFile = new File(dir, System.currentTimeMillis()+"");
		FileOutputStream fos = new FileOutputStream(compactFile);
		
		for(File split : splits) {
			byte[] b = new byte[(int)split.length()];
			FileInputStream fisSplit = new FileInputStream(split);
			int read = fisSplit.read(b);
			fos.write(b, 0, read);
			fisSplit.close();
			fos.flush();
			split.delete();
		}
		
		fos.close();
//		System.out.println("read use time "+(System.currentTimeMillis()-start));
		return compactFile;
	}
	
	private static List<File> rwFileConcurrent(File f, boolean deleteSplitedFiles) throws InterruptedException, ExecutionException, IOException {
		
		long start = System.currentTimeMillis();
		
		List<File> splitedFiles = new ArrayList<File>();
		
		FileInputStream fis = new FileInputStream(f);
		FileChannel fisChannel = fis.getChannel();
		
		
		AtomicLong offset = new AtomicLong(0);
		
		int num = (int)(f.length()/splitSize);
		List<Future<File>> flist = new ArrayList<Future<File>>();
		for(int i=0; i<num; i++) {
			Future<File> submit = es.submit(new SplitReader(offset,fisChannel));
			flist.add(submit);
		}
		
		for(Future<File> future : flist) {
			File splitedFile = future.get();
			splitedFiles.add(splitedFile);
			if(deleteSplitedFiles) 
				splitedFile.delete();
		}
		System.out.println("	concurrent "+corePoolSize+" fileSize "+fileSize+" RW use time "+(System.currentTimeMillis()-start));
		fis.close();
		f.delete();
		return splitedFiles;
	}
	 
	
	
	
	static class SplitReader implements Callable<File> {
		AtomicLong offset ;
		FileChannel fisChannel;
		public SplitReader( AtomicLong offset, FileChannel fisChannel) {
			this.offset = offset;
			this.fisChannel = fisChannel;
		}

		@Override
		public File call() throws Exception {
			long start = System.currentTimeMillis();
			int  offset2 = 0;
			int length = 0;
			synchronized(offset) {
				offset2 = (int)offset.get();
				long srcFileSize = fisChannel.size();
				if( (srcFileSize-offset2) >= splitSize) {
					offset.set(offset2 + splitSize);
					length = splitSize;
				} else {
					offset.set(offset2 + (srcFileSize-offset2));
					length = (int)(srcFileSize-offset2);
				}
				
				offset.notifyAll();
			}
				
				File ff = new File(dir,""+offset);

				FileOutputStream fos = new FileOutputStream(ff);
				FileChannel fosChannel = fos.getChannel();
//				int index = 0;
//				int batchSize = length;
//				
				fisChannel.transferTo(offset2, length, fosChannel);
//				while((length-index)>0) {
//					fos.write(srcFileBytes,offset2, length-index>=batchSize? batchSize : length-index);
//					fos.flush();
//					index = index + (length-index>=batchSize? batchSize : length-index);
//				}
				fos.flush();
				fos.close();
				fosChannel.close();
//				System.out.print(ff);
//				System.out.println(" write 1 file use time "+(System.currentTimeMillis()-start));
				return ff;
		}
		
	}

}
