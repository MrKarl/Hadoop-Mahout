package com.showindow.userbased;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class EvaluatorWithFileIO {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException, TasteException {
		// TODO Auto-generated method stub
		
		final Long userID = Long.parseLong(args[0]);
		final int numOfRecommend = Integer.parseInt(args[1]);
		
//		final long userID = (long) 14;
//		final int numOfRecommend = 10;
		
		DataModel model = new FileDataModel(new File("/tmp/favor.csv"));
//		DataModel model = new FileDataModel(new File("input/favor.csv"));
		
		
		RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		
		RecommenderBuilder builder = new RecommenderBuilder() {
			public Recommender buildRecommender(DataModel model) throws TasteException{
				
				UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
				UserNeighborhood neighborhood = new NearestNUserNeighborhood(5, similarity, model);
//				UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.7, similarity, model);
				
				Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity); 
				
				
				/*
				for(LongPrimitiveIterator users = model.getUserIDs(); users.hasNext();) {
					long userID = users.nextLong();
					List<RecommendedItem> recommendations = recommender.recommend(userID, 5);
					for(RecommendedItem recommendation : recommendations) {
						System.out.println("userID =" + userID + " / items =>" + recommendation.getItemID() + "," + recommendation.getValue());;						
					}
					
				}*/
				
			
				List<RecommendedItem> recommendations = recommender.recommend(userID, numOfRecommend);
//				System.out.println(recommendations);
				
//				System.out.println("userID =" + userID + " / items =>" + recommendations.get(0).getItemID() + "," + recommendations.get(0).getValue());;
				int i = 0;
				for(RecommendedItem recommendation : recommendations) {
					
					System.out.println(i++ + ". userID =" + userID + " / items =>" + recommendation.getItemID() + "," + recommendation.getValue());;						
				}
				
				return recommender;
//				return new SlopeOneRecommender(model);
			}
		};
		
		double score = evaluator.evaluate(builder, null, model, 0.7, 1.0); // 데이터의 70%를 학습에 사용, 데이터의 30%는 테스트에 사용. 
		
		System.out.println(score);
	}
}
