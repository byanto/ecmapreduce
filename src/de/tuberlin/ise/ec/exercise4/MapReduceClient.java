package de.tuberlin.ise.ec.exercise4;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

/**
 * Amazon Web Service Elastic MapReduce exercise.
 * 
 * @author markus klems
 * 
 */
public class MapReduceClient {

	// TODO
	private static final String CLUSTER_ID = "..."; // e.g., "j-1HTE8WKS7SODR"
	private static final String PATH_TO_JAR_IN_S3 = "..."; // "s3://<yourbucket>/ecmapreduce-1.0.0.jar"
	private static final String PATH_TO_INPUT = "s3n://elasticmapreduce/samples/wordcount/input"; // dont change this
	private static final String PATH_TO_OUTPUT_1 = "..."; // "s3://<yourbucket>/wordcount/output"
	private static final String PATH_TO_OUTPUT_2 = "..."; // "s3://<yourbucket>/wordcountaba/output"
	private static final String PATH_TO_OUTPUT_3 = "..."; // "s3://<yourbucket>/palindromecount/output"

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
		HadoopJarStepConfig wordCountStep = new HadoopJarStepConfig()
				.withJar(PATH_TO_JAR_IN_S3)
				.withMainClass("de.tuberlin.ise.ec.exercise4.WordCount")
				.withArgs(PATH_TO_INPUT, PATH_TO_OUTPUT_1);
		HadoopJarStepConfig wordCountABAStep = new HadoopJarStepConfig()
				.withJar(PATH_TO_JAR_IN_S3)
				.withMainClass("de.tuberlin.ise.ec.exercise4.WordCountABA")
				.withArgs(PATH_TO_INPUT, PATH_TO_OUTPUT_2);
		HadoopJarStepConfig palindromeCountStep = new HadoopJarStepConfig()
				.withJar(PATH_TO_JAR_IN_S3)
				.withMainClass("de.tuberlin.ise.ec.exercise4.PalindromeCount")
				.withArgs(PATH_TO_INPUT, PATH_TO_OUTPUT_3);
		StepConfig wordCountStepConfig = new StepConfig("WordCount",
				wordCountStep);
		StepConfig wordCountABAStepConfig = new StepConfig("WordCountABA",
				wordCountABAStep);
		StepConfig palindromeCountStepConfig = new StepConfig(
				"PalindromeCount", palindromeCountStep);

		// TODO add the missing steps
		AddJobFlowStepsResult result = client
				.addJobFlowSteps(new AddJobFlowStepsRequest().withJobFlowId(
						CLUSTER_ID).withSteps(palindromeCountStepConfig));
		System.out.println(result.getStepIds());

	}

}
