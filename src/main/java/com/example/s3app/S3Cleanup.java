package com.example.s3app;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.xdesign.munro.entity.Mountain;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class S3Cleanup {
    private static final int MAX_NUMBER_OF_KEYS = 5;

	public static void main(String[] args) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_2).build();
        List<Bucket> buckets = s3.listBuckets();
        for (Bucket b : buckets) {
        	int numberOfKeys = countNumberOfKeys(s3,b.getName());
        	System.out.println("bucketName: [" + b.getName()+"]");
        	System.out.println("numberOfKeys: [" + numberOfKeys+"]");
        	
        	if (numberOfKeys > MAX_NUMBER_OF_KEYS) {
        		cleanOldKeys(s3,b.getName());
        	}
        }
    }
    
    private static int countNumberOfKeys(AmazonS3 s3,String bucketName) {
        ListObjectsV2Result result = s3.listObjectsV2(bucketName);
		return result.getKeyCount();
	}

	private static void cleanOldKeys(AmazonS3 s3,String bucketName) {
        ListObjectsV2Result result = s3.listObjectsV2(bucketName);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        Stream<S3ObjectSummary> stream = objects.stream();
        
        stream = stream.sorted(Comparator.comparing(S3ObjectSummary::getLastModified));
        
        /* Reverse Order */
		List<S3ObjectSummary> lst = stream.collect(Collectors.toList());
		Collections.reverse(lst);
		stream = lst.stream();
		
		/* Max Keys */
		stream = stream.limit(MAX_NUMBER_OF_KEYS);
		
		/* Stream to List*/
        List<S3ObjectSummary> listWithMaxObjects = stream.collect(Collectors.toList());
        
        
        for (S3ObjectSummary os : listWithMaxObjects) {
            System.out.println("    * " + os.getKey() + "["+os.getLastModified()+"]");
        }
    }
}