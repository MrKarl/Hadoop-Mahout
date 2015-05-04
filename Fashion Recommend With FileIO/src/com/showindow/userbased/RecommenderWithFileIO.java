package com.showindow.userbased;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class RecommenderWithFileIO {

	/**
	 * @param args
	 * @throws IOException
	 * @throws TasteException
	 */
	public static void main(String[] args) throws IOException, TasteException {
		// TODO Auto-generated method stub

		// FOR SERVICE
//		 final Long userID = Long.parseLong(args[0]);
//		 final int numOfRecommend = Integer.parseInt(args[1]);
//		 DataModel model = new FileDataModel(new File("/tmp/favor.csv"));

		// FOR TEST
		final long userID = (long) 14;
		final int numOfRecommend = 10;
		DataModel model = new FileDataModel(new File("input/favor1.csv"));

		UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(5,
				similarity, model);
		// UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.7,
		// similarity, model);

		Recommender recommender = new GenericUserBasedRecommender(model,
				neighborhood, similarity);
		recommender.refresh(null);

		List<RecommendedItem> recommendations = recommender.recommend(userID,
				numOfRecommend);
		
		System.out.println(userID);
		
//		RECOMMENDATION
		System.out.println("%%%%%%%%%%");
		

		for (RecommendedItem recommendation : recommendations) {
//			System.out.println("item:" + recommendation.getItemID()
//					+ ",rating:" + recommendation.getValue());
			
			System.out.println(recommendation.getItemID()
					+ ","+ recommendation.getValue());
		}
		
//		MY CHOICE
		System.out.println("%%%%%%%%%%");

		PreferenceArray v = model.getPreferencesFromUser(userID);
		// v.sortByValue();
		v.sortByValueReversed();
		for (Preference vs : v) {
//			System.out.println("item:" + vs.getItemID() + ",rating:" + vs.getValue());
			System.out.println(vs.getItemID() + "," + vs.getValue());			
		}
				
//		MY NEIGHBOR
		System.out.println("%%%%%%%%%%");
		long[] neighborhoods = neighborhood.getUserNeighborhood(userID);

		for (long n : neighborhoods) {
			System.out.println(n);
		}
		JSONObject obj = new JSONObject(); // FOR MOBILE CONNECTION

		// FOR TEST OUTPUT
		// System.out.println(model.getNumItems());
		// System.out.println(model.getNumUsers());

	}

}
