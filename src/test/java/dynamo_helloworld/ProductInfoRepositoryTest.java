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

import java.io.IOException;
import java.util.List;

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

    @Autowired
    CompressHelper compressHelper;

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


    @Test
    public void sampleTestCase() throws IOException {
        ProductInfo dave = new ProductInfo();
        dave.setContent(compressHelper.compressString("huge string to be compressed"));
        dave.setCost(EXPECTED_COST);
        dave.setMsrp(EXPECTED_PRICE);

        productInfoRepository.save(dave);

        List<ProductInfo> result
                = (List<ProductInfo>) productInfoRepository.findAll();

        assertTrue("Not empty", result.size() > 0);
        assertTrue("Contains item with expected cost",
                result.get(0).getCost().equals(EXPECTED_COST));

        assertThat(compressHelper.uncompressString(result.get(0).getContent()), is("huge string to be compressed"));
    }
}