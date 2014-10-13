package de.tuberlin.ise.ec.exercise4;

import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.*;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

/**
 * Amazon Web Service Elastic MapReduce exercise.
 * 
 * @author markus klems
 * 
 */
public class MapReduceClient {

	// TODO
	private static final String CLUSTER_ID = "j-17Z9IQI4Z0JFQ"; // e.g., "j-1HTE8WKS7SODR"
	private static final String PATH_TO_JAR_IN_S3 = "s3://tuberliniseec/ecmapreduce-1.0.0.jar"; // e.g., "s3://mybucket/my-jar-location1"

	public static void main(String[] args) {

		/*
		 * The ProfileCredentialsProvider will return your [default] credential
		 * profile by reading from the credentials file located at
		 * (<pathToYourUserHome>/.aws/credentials).
		 */
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default")
					.getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location (/<path-to-user-home>/.aws/credentials), and is in valid format.",
					e);
		}

		AmazonElasticMapReduce client = new AmazonElasticMapReduceClient(
				credentials);

		// A custom step
		HadoopJarStepConfig hadoopConfig1 = new HadoopJarStepConfig()
				.withJar(PATH_TO_JAR_IN_S3)
				.withMainClass("de.tuberlin.ise.ec.exercise4.WordCount")
				.withArgs("--verbose"); // optional list of arguments
		StepConfig customStep = new StepConfig("Step1", hadoopConfig1);

		AddJobFlowStepsResult result = client
				.addJobFlowSteps(new AddJobFlowStepsRequest()
						.withJobFlowId(CLUSTER_ID).
						withSteps(customStep));
		System.out.println(result.getStepIds());

	}

}
