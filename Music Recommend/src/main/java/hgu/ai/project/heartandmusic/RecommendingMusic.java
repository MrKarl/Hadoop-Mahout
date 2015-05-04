package hgu.ai.project.heartandmusic;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class RecommendingMusic {

	public static void main(String[] args) throws IOException, TasteException {
		// TODO Auto-generated method stub

		//		FOR SERVICE
//		final Long userID = Long.parseLong(args[0]);
//		final int numOfRecommend = Integer.parseInt(args[1]);

		
		//		FOR TEST
		final long userID = (long) 2;
		final int numOfRecommend = 10;
		
		DataModel model = new FileDataModel(new File("datas/tb_user_artists.csv"));
		UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(5,	similarity, model);
		// UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.7, similarity, model);

		Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
		recommender.refresh(null);

		List<RecommendedItem> recommendations = recommender.recommend(userID, numOfRecommend);		

		
		System.out.println("User"+ userID + "님께 추천하는 아이템 및 추천점수");
		for (RecommendedItem recommendation : recommendations) {
			System.out.println("item:" + recommendation.getItemID()	+ "\t rating:" + recommendation.getValue());			
//			System.out.println(recommendation.getItemID() + ","+ recommendation.getValue());//
		}		
		

//		MY CHOICE
//		System.out.println("%%%%%%%%%%");

//		PreferenceArray v = model.getPreferencesFromUser(userID);
//		v.sortByValueReversed();  // v.sortByValue();

//		for (Preference vs : v) {
//			System.out.println("item:" + vs.getItemID() + ",rating:" + vs.getValue());
//			System.out.println(vs.getItemID() + "," + vs.getValue());			
//		}
		
//		MY NEIGHBOR
//		long[] neighborhoods = neighborhood.getUserNeighborhood(userID);
//		for (long n : neighborhoods) {
//			System.out.println(n);
//		}

//		JSONObject obj = new JSONObject(); // FOR MOBILE CONNECTION
	}
}
