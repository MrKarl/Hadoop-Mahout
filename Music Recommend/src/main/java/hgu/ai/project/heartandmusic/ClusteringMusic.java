package hgu.ai.project.heartandmusic;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.clustering.fuzzykmeans.FuzzyKMeansDriver;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.json.simple.JSONObject;

import com.opencsv.CSVReader;

public class ClusteringMusic {
	private static String basePath = "datas";
    private static String inputDataPath = basePath + "/input";
    private static String outputDataPath = basePath+ "/output";
    private static String baseClusterPath = basePath+"/clusters";
    private static int clusterCount = 9;
    private Configuration conf;
    private FileSystem fs;
    
    public ClusteringMusic(){
        try {
            this.conf = getConfiguration();
            this.fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writePointsToFile(List<NamedVector> points, String fileName) throws IOException {
        Path path = new Path(fileName);
        SequenceFile.Writer writer = new SequenceFile.Writer(this.fs, this.conf,  path,  Text.class, 
        													VectorWritable.class);
        VectorWritable vec = new VectorWritable();
        for(NamedVector point : points){
            vec.set(point);
            writer.append(new Text(point.getName()), vec);
        }
        writer.close();
    }

    private void writeInitialCenterPoints(List<NamedVector> points) throws IOException {
        
        Path path = new Path(baseClusterPath+"/part-00000");
        SequenceFile.Writer writer = new SequenceFile.Writer(this.fs,  this.conf, path, Text.class, 
        		Cluster.class);
        
        for(int i=0;i<clusterCount;i++){
            Vector vec = points.get(i);
            Cluster cluster = new Cluster(vec, i, new EuclideanDistanceMeasure());
            writer.append(new Text(cluster.getIdentifier()), cluster);
        }
        writer.close();
    }
    
    private List<NamedVector> getPoints(String filename) throws NumberFormatException, IOException{
    	// For Vectorization
        List<NamedVector> artists = new ArrayList<NamedVector>();
        NamedVector artist;
		
		CSVReader reader = new CSVReader(new FileReader( basePath + "/"+ filename));//tb_artists_energy_bright.csv"));
		String[] nextLine;
		while ((nextLine = reader.readNext()) != null) {
/**			nextLine[] is an array of values from the line
/*			nextLine[0] : artistID		nextLine[1] : artistName
/*			nextLine[2] : artistSongs	nextLine[3] : artistPicture
/*			nextLine[4] : energy		nextLine[5] : bright
 */
			artist = new NamedVector(new DenseVector(
					 new double[]{Double.parseDouble(nextLine[4]),
					              Double.parseDouble(nextLine[5])}),
					nextLine[0]);
			
			artists.add(artist);			
		}
        return artists;
    }
    
    private Configuration getConfiguration() throws IOException{
        Configuration conf = new Configuration();
/**       for HDFS setting 입니다.
/*        Resource resource1 = new ClassPathResource("/core-site.xml");
/*        Resource resource2 = new ClassPathResource("/mapred-site.xml");
/*        Resource resource3 = new ClassPathResource("/hdfs-site.xml");
/*        conf.addResource(resource1.getURL());
/*        conf.addResource(resource2.getURL());
/*        conf.addResource(resource3.getURL());
 */
        return conf;
    }
    
    public void execute(String filename) throws Exception{
        //데이터 세팅
        List<NamedVector> vectors = getPoints(filename);
        
        // sequence file 생성
        writePointsToFile(vectors, inputDataPath+"/file1"); 
        
        // 초기 기준 점 찍기
        writeInitialCenterPoints(vectors);  
        
        // 실행 
        FuzzyKMeansDriver.run(
        		conf,								// Configuration
        		new Path(inputDataPath),			// Data Input Path
        		new Path(baseClusterPath),			// Clusters Input path
        		new Path(outputDataPath),			// Data Output Path
                new EuclideanDistanceMeasure(),		// DistanceMeasure
                0.001,								// convergenceDelta(수렴임계)
                10,									// maxIterations
                2,									// m - the double "fuzzyness" argument (>1)
                true, 								// runClustering
                true,								// emitMostLikely
                2,									// threshold
                false);								// runSequential
    }

    @SuppressWarnings("unchecked")
	public JSONObject confirmResult() throws Exception{    
    	JSONObject obj;

    	obj = new JSONObject();

    	SequenceFile.Reader reader = new SequenceFile.Reader(fs,  
    			new Path(outputDataPath+"/"+Cluster.CLUSTERED_POINTS_DIR+"/part-m-00000"), conf);
        IntWritable key = new IntWritable();
        WeightedVectorWritable value = new WeightedVectorWritable();
        while(reader.next(key, value)){
            NamedVector v = (NamedVector)value.getVector();
            
            obj.put(v.getName(), key.toString()+":"+
            		value.getVector().get(0) + "/" +value.getVector().get(1));
            
            System.out.println(v.getName() +"th item is in "+ (key.toString()+1)+"-Cluster, and these values are ["+value.getVector().get(0) + ", " +value.getVector().get(1)+"]");
            
        }
        reader.close();
        
        return obj;
    }

    public static void main(String[] args) throws Exception{
    	
    	String filename = "tb_artists_energy_bright.csv";
    	ClusteringMusic test = new ClusteringMusic();
        test.execute(filename);
        
        JSONObject obj = test.confirmResult();		// JSON 으로 주어진 객체를 어느곳에든 쓸 수 있음.
    }
}
