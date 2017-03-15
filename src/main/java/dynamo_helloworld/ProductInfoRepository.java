package dynamo_helloworld;

import org.socialsignin.spring.data.dynamodb.repository.DynamoDBCrudRepository;
import org.socialsignin.spring.data.dynamodb.repository.EnableScan;
import org.springframework.stereotype.Repository;

import java.util.List;

@EnableScan
@Repository
public interface ProductInfoRepository extends
        DynamoDBCrudRepository<ProductInfo, String> {

    List<ProductInfo> findById(String id);
}
