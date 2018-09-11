package nearsoft.academy.bigdata.recommendation;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class MovieRecommender {


    private HashBiMap<String, Integer> prod;
    private final GenericUserBasedRecommender rec;
    private int totalReviews;
    private HashBiMap<String, Integer> usr;

    public MovieRecommender(String dataSrc) throws IOException, TasteException {
        DataModel model = new FileDataModel(new File(convertSourceToCsv(dataSrc)));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
        rec = new GenericUserBasedRecommender(model, neighborhood, similarity);

    }


    List<String> getRecommendationsForUser(String usrName) throws TasteException {
        System.out.println("recommendations for user 1 - " + usr.inverse().get(1));
        List<RecommendedItem> recommendations = rec.recommend(usr.get(usrName), 3);
        List<String> recommendedMovies = new ArrayList<>();
        BiMap<Integer, String> productsByName = prod.inverse();
        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);
            String movieName = productsByName.get((int)recommendation.getItemID());
            System.out.println("movie:"+movieName);
            recommendedMovies.add(movieName);
        }
        return recommendedMovies;
    }

    private String convertSourceToCsv(String dataSrc) {
        try {
            File source = new File(dataSrc);
            usr = HashBiMap.create ();
            prod = HashBiMap.create();
            File temp = new File(source.getParentFile().getAbsolutePath() + "/tempdata.csv");
            if (temp.exists()) {
                temp.delete();
            }
            else{
                temp.createNewFile();
            }
            try (InputStream fileStream = new FileInputStream(dataSrc);
                 InputStream gzipStream = new GZIPInputStream(fileStream);
                 Reader decoder = new InputStreamReader(gzipStream, "UTF8");
                 BufferedReader buffered = new BufferedReader(decoder);
                 Writer writer = new BufferedWriter(new FileWriter(temp));) {
                String str;
                Integer userId = null;
                String score = "";
                Integer productId = null;
                totalReviews = 0;
                boolean readingRecord = false;
                while ((str = buffered.readLine()) != null) {
                    if (readingRecord) {
                        if (str.contains("review/userId")) {
                            String userName =getValuePortionOfString(str);
                            if(!usr.containsKey(userName)){
                                usr.put(userName, usr.size()+1);
                            }
                            userId = usr.get(userName);
                        } else if (str.contains("review/score")) {
                            score = getValuePortionOfString(str);
                        } else if (str.contains("review/summary")) {
                            writer.append(String.valueOf(userId));
                            writer.append(",");
                            writer.append(String.valueOf(productId));
                            writer.append(",");
                            writer.append(score);
                            writer.append("\n");
                            userId = null;
                            score = "";
                            productId = null;
                            readingRecord = false;
                        }
                    } else if (str.contains("product/productId")) {
                        String productName = getValuePortionOfString(str);
                        if(!prod.containsKey(productName)){
                            prod.put(productName, prod.size()+1);
                        }
                        productId = prod.get(productName);
                        readingRecord = true;
                        totalReviews++;
                    }
                }
            }
            return temp.getAbsolutePath();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error processing the data source " + dataSrc, e);
        }
    }

    private String getValuePortionOfString(String str) {
        return str.substring(str.indexOf(":") + 2, str.length());
    }

    int getTotalProducts(){
        return prod.size();
    }

    int getTotalUsers(){
        return usr.size();
    }


    int getTotalReviews() {
        return totalReviews;
    }

    public void setTotalReviews(int totalReviews) {
        this.totalReviews = totalReviews;
    }
}