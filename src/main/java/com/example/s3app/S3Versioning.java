package com.example.s3app;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;

public class S3Versioning {

	public static void main(String[] args) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_2).build();
        
        goThroughVersionsInBucket(s3,bucketName());
        
        /*
        List<Bucket> buckets = s3.listBuckets();
        for (Bucket b : buckets) {
        	System.out.println("--------------------------------------------");
        	int numberOfKeys = countNumberOfKeys(s3,b.getName());
        	System.out.println("bucketName: [" + b.getName()+"]");
        	System.out.println("numberOfKeys: [" + numberOfKeys+"]");
        	
        	goThroughVersionsInBucket(s3,b.getName());
        }
        */
    }

	private static String bucketName() {
		return "code-deploy-example";
	}
    
	private static void goThroughVersionsInBucket(AmazonS3 s3,String bucketName) {
		ListVersionsRequest request = new ListVersionsRequest().withBucketName(bucketName);
		VersionListing versionListing = s3.listVersions(request);
		
		List<S3VersionSummary> keyVersions;
		List<String> keys = listAllKeysForBucket(s3, bucketName);		
		for (String key : keys) {
			keyVersions = new ArrayList<S3VersionSummary>();
			System.out.println("--------------------------------------------");
			System.out.println(" Key: " + key + "  " );
	        for (S3VersionSummary objectSummary :  versionListing.getVersionSummaries()) {
	        	if (key.equalsIgnoreCase(objectSummary.getKey())) {
	        		System.out.println(" 	- Version: " + objectSummary.getKey() + "  " +
	                    //"(size = " + objectSummary.getSize() + ")" +
	                    "(" + objectSummary.getVersionId() + ") "+
	                    "("+objectSummary.getLastModified()+")"
	                    );
	        		keyVersions.add(objectSummary);	
	        	}
	        	
	        }	
	        Stream<S3VersionSummary> stream = keyVersions.stream();
	        stream = stream.sorted(Comparator.comparing(S3VersionSummary::getLastModified));
			List<S3VersionSummary> lst = stream.collect(Collectors.toList());
			
			if (lst.size() > 0) {
				S3VersionSummary firstVersion = lst.get(0);
				System.out.println("\n>> First Version: " + firstVersion.getKey() + "  " +
						"(" + firstVersion.getVersionId() + ") "+
						"("+firstVersion.getLastModified()+") \n" );
			}
		
		}
		
    }

	private static List<String> listAllKeysForBucket(AmazonS3 s3, String bucketName) {
		ListObjectsV2Result result = s3.listObjectsV2(bucketName);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        
        List<String> keys = new ArrayList<String>();
        for (S3ObjectSummary os : objects) {
            keys.add(os.getKey());
        }
        return keys;
	}
	
    private static int countNumberOfKeys(AmazonS3 s3,String bucketName) {
        ListObjectsV2Result result = s3.listObjectsV2(bucketName);
		return result.getKeyCount();
	}
    	
	
}