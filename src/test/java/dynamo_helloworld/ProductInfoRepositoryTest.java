package dynamo_helloworld;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {DynamoHelloWorldApplication.class,DynamoConfiguration.class,ProductInfoRepository.class})
@WebAppConfiguration
@ActiveProfiles("local")
@TestPropertySource(properties = {
        "amazon.dynamodb.endpoint=http://localhost:8000/",
        "amazon.aws.accesskey=test1",
        "amazon.aws.secretkey=test231" })
public class ProductInfoRepositoryTest {
    private DynamoDBMapper dynamoDBMapper;

    @Autowired
    private AmazonDynamoDB amazonDynamoDB;

    @Autowired
    ProductInfoRepository productInfoRepository;

    private static final String EXPECTED_COST = "20";
    private static final String EXPECTED_PRICE = "50";

    @Before
    public void setup() throws Exception {
        dynamoDBMapper = new DynamoDBMapper(amazonDynamoDB);

        CreateTableRequest tableRequest = dynamoDBMapper
                .generateCreateTableRequest(ProductInfo.class);
        tableRequest.setProvisionedThroughput(
                new ProvisionedThroughput(1L, 1L));

        amazonDynamoDB.deleteTable("ProductInfo");

        amazonDynamoDB.createTable(tableRequest);

        //...

        dynamoDBMapper.batchDelete(
                (List<ProductInfo>)productInfoRepository.findAll());
    }

    private static ByteBuffer compressString(String input) throws IOException {
        // Compress the UTF-8 encoded String into a byte[]
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream os = new GZIPOutputStream(baos)) {

            os.write(input.getBytes("UTF-8"));
            os.close();
            //baos.close();
            byte[] compressedBytes = baos.toByteArray();

            // The following code writes the compressed bytes to a ByteBuffer.
            // A simpler way to do this is by simply calling ByteBuffer.wrap(compressedBytes);
            // However, the longer form below shows the importance of resetting the position of the buffer
            // back to the beginning of the buffer if you are writing bytes directly to it, since the SDK
            // will consider only the bytes after the current position when sending data to DynamoDB.
            // Using the "wrap" method automatically resets the position to zero.
            ByteBuffer buffer = ByteBuffer.allocate(compressedBytes.length);
            buffer.put(compressedBytes, 0, compressedBytes.length);
            buffer.position(0); // Important: reset the position of the ByteBuffer to the beginning
            return buffer;
        }

    }

    private static String uncompressString(ByteBuffer input) throws IOException {
        byte[] bytes = input.array();

        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPInputStream is = new GZIPInputStream(bais)) {

            int chunkSize = 1024;
            byte[] buffer = new byte[chunkSize];
            int length = 0;
            while ((length = is.read(buffer, 0, chunkSize)) != -1) {
                baos.write(buffer, 0, length);
            }

            return new String(baos.toByteArray(), "UTF-8");

        }

    }

    @Test
    public void sampleTestCase() throws IOException {
        ProductInfo dave = new ProductInfo();
        dave.setContent(compressString("huge string to be compressed"));
        dave.setCost(EXPECTED_COST);
        dave.setMsrp(EXPECTED_PRICE);

        productInfoRepository.save(dave);

        List<ProductInfo> result
                = (List<ProductInfo>) productInfoRepository.findAll();

        assertTrue("Not empty", result.size() > 0);
        assertTrue("Contains item with expected cost",
                result.get(0).getCost().equals(EXPECTED_COST));

        assertThat(uncompressString(result.get(0).getContent()), is("huge string to be compressed"));
    }
}