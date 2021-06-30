package com.bjsxt.test;

import com.bjsxt.mongo.SpringDataMongoDBApplication;
import com.bjsxt.mongo.pojo.Order;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * 用document 去新增,修改,删除操作
 * 用BasicQuery 去查询
 */
@SpringBootTest(classes = {SpringDataMongoDBApplication.class})
@RunWith(SpringRunner.class)
public class TestSpringDataMongoDB2 {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Test
    public void testDocument() {
//        Document document = Document.parse("{\n" +
//                "    \"_id\": \"3\",\n" +
//                "    \"name\": \"zhangsan\"\n" +
//                "}");

        Document document = new Document();
        document.put("_id", "3");
        document.put("name", "zhangsan");
        System.out.println(document);
    }

    /**
     * 新增
     */
    @Test
    public void testInsert() {
        Document document = Document.parse("{\n" +
                "    \"_id\": \"199\",\n" +
                "    \"name\": \"199\"\n" +
                "}");
        mongoTemplate.insert(document, "test");
    }

    /**
     * 更新,
     * 好像只能用save方法进行全量更新
     */
    @Test
    public void testUpdate() {
        Document document = new Document();
        document.put("_id", "100");
//        document.put("name","lisi");
//        document.put("age",22);

        mongoTemplate.save(document, "test");
    }

    /**
     * 查询一个
     */
    @Test
    public void testQueryOne() {
//        BasicQuery basicQuery = new BasicQuery("{_id:\"3\"}");

//        Document document = Document.parse("{_id:\"3\"}");

        Document document = new Document();
        document.put("_id", "3");
//        document.put("_id", Document.parse("{$eq:\"3\"}"));
        BasicQuery basicQuery = new BasicQuery(document);

        List<Document> documents = mongoTemplate.find(basicQuery, Document.class, "order");
        documents.stream().forEach(d -> System.out.println(d.toJson()));
    }

    /**
     * 条件查询 -
     * 不等值
     */
    @Test
    public void testFindNotEqual() {
        // 不等值查询， title != "测试新增1"
//        Query query2 = Query.query(Criteria.where("title").ne("测试新增2"));

        // document + query
        Document document = new Document();
        document.put("title", Document.parse("{$ne:\"测试新增2\"}"));
        BasicQuery basicQuery = new BasicQuery(document);

        // BasicQuery
//        BasicQuery basicQuery = new BasicQuery("{title:{$ne:\"测试新增2\"}}");

        List<Order> orders2 = mongoTemplate.find(basicQuery, Order.class);
        orders2.stream().forEach(System.out::println);
    }

    /**
     * 条件查询 -
     * 范围
     */
    @Test
    public void testFindRange() {
        // 不等值查询， 范围条件  100 < payment < 300
//        Query query3 = Query.query(Criteria.where("payment").gt(100).lt(300));

        // document + query
//        Document document = new Document();
//        document.put("payment",Document.parse("{$gt:100}"));
//        document.put("payment",Document.parse("{$lt:500}"));
//        BasicQuery basicQuery = new BasicQuery(document);

        // BasicQuery
        BasicQuery basicQuery = new BasicQuery("{payment:{$gt:100}}");

        List<Order> orders = mongoTemplate.find(basicQuery, Order.class);
        orders.stream().forEach(System.out::println);
    }

    /**
     * 条件查询
     * in
     */
    @Test
    public void testFindListByIn() {
        // in ， title in ("测试新增1", "测试新增2")
//        Query query1 = Query.query(Criteria.where("title").in("测试新增1", "测试新增2"));

        // document + query
        Document document = new Document();
        document.put("title", Document.parse("{$in:[\"测试新增2\",\"测试新增3\"]}"));
        BasicQuery basicQuery = new BasicQuery(document);

        //BasicQuery
//        BasicQuery basicQuery = new BasicQuery("{title:{$in:[\"测试新增2\",\"测试新增3\"]}}");

        List<Order> orders = mongoTemplate.find(basicQuery, Order.class);
        orders.stream().forEach(System.out::println);
    }

    /**
     * 条件查询
     * not in
     */
    @Test
    public void testFindListByNotIn() {
        // not in , title not in ("测试新增1", "测试新增2")
//        Query query2 = Query.query(Criteria.where("title").nin("测试新增1", "测试新增2"));

        // document + query
//        Document document = new Document();
//        document.put("title", Document.parse("{$nin:[\"测试新增2\",\"测试新增3\"]}"));
//        BasicQuery basicQuery = new BasicQuery(document);

        //BasicQuery
        BasicQuery basicQuery = new BasicQuery("{title:{$nin:[\"测试新增2\",\"测试新增3\"]}}");

        List<Order> orders = mongoTemplate.find(basicQuery, Order.class);
        orders.stream().forEach(System.out::println);
    }

    /**
     * 条件查询
     * exists
     */
    @Test
    public void testFindListByExists() {
        // exists ， payment exists
//        Query query = Query.query(Criteria.where("payment").exists(true));

        // document + query
        Document document = new Document();
        document.put("payment", Document.parse("{$exists:true}"));
        BasicQuery basicQuery = new BasicQuery(document);

        // BasicQuery
//        BasicQuery basicQuery = new BasicQuery("{payment:{$exists:true}}");

        List<Order> orders = mongoTemplate.find(basicQuery, Order.class);
        orders.stream().forEach(System.out::println);
    }

    /**
     * 条件查询
     * 复合条件， and
     */
    @Test
    public void testFindListByAnd() {
        // 多个条件字段不同，使用andOperator实现
//        Query query = new Query();
//        Criteria criteria = new Criteria().andOperator(
//                Criteria.where("title").regex("测试"),
//                Criteria.where("payment").gt(300),
//                Criteria.where("payment").lt(350)
//        );
//        query.addCriteria(criteria);

        // document + query
        Document document = new Document();
        // 这样写会报错 要么json转换异常,要么array异常
//        document.put("$and", Document.parse("{[{payment:{$gt:100}},{payment:{$lt:400}}]}"));
        List<Document> list = new ArrayList<>();
        list.add(Document.parse("{payment:{$gt:100}}"));
        list.add(Document.parse("{payment:{$lt:400}}"));
        document.put("$and", list);
        BasicQuery query = new BasicQuery(document);

        // BasicQuery
        BasicQuery query2 = new BasicQuery("{$and:[{payment:{$gt:100}},{payment:{$lt:400}}]}");

        List<Order> orders = mongoTemplate.find(query, Order.class);
        orders.stream().forEach(System.out::println);
    }


    /**
     * 条件查询
     * 复合条件， or
     * 使用orOperator实现
     */
    @Test
    public void testFindListByOr() {
//        Query query = new Query();
//        Criteria criteria = new Criteria().orOperator(
//                Criteria.where("title").regex("2$"),
//                Criteria.where("payment").gt(300)
//        );
//        query.addCriteria(criteria);

        //document + query
        Document document = new Document();
        List<Document> list = new ArrayList<>();
        list.add(Document.parse("{payment:{$gte:400}}"));
        list.add(Document.parse("{payment:{$lte:100}}"));
        document.put("$or", list);
        BasicQuery query = new BasicQuery(document);

        // BasicQuery
//        BasicQuery query = new BasicQuery("{$or:[{payment:{$gte:400}},{payment:{$lte:100}}]}");

        List<Order> orders = mongoTemplate.find(query, Order.class);
        orders.stream().forEach(System.out::println);
    }


    /**
     * 条件查询
     * 复合条件， and + or
     * 注意嵌套
     */
    @Test
    public void testFindListByAndOr() {
//        Criteria and1 = new Criteria().andOperator(
//                Criteria.where("title").is("测试新增2"),
//                Criteria.where("payment").lte(300)
//        );
//
//        Criteria and2 = new Criteria().andOperator(
//                Criteria.where("title").is("测试新增4"),
//                Criteria.where("payment").gte(400)
//        );
//        Query query = Query.query(
//                new Criteria().orOperator(
//                        and1, and2
//                )
//        );

        // document + query
        Document document = new Document();
        List<Document> list = new ArrayList<>();
        list.add(Document.parse("{$and:[{title:{$eq:\"测试新增2\"}},{payment:{$lte:300}}]}"));
        list.add(Document.parse("{$and:[{title:{$eq:\"新增测试4\"}},{payment:{gte:400}}]}"));
        document.put("$or", list);
        BasicQuery query = new BasicQuery(document);

        // BasicQuery
//        BasicQuery query = new BasicQuery("{$or:[" +
//                "{$and:[{title:{$eq:\"测试新增2\"}},{payment:{$lte:300}}]}," +
//                "{$and:[{title:{$eq:\"新增测试4\"}},{payment:{gte:400}}]}" +
//                "]}");

        List<Order> orders = mongoTemplate.find(query, Order.class);
        orders.stream().forEach(System.out::println);
    }


    /**
     * 排序
     */
    @Test
    public void testSort() {
//        Query query = new Query();
//        Sort sort = Sort.by(Sort.Direction.DESC, "title");
//        query.with(sort);

        BasicQuery query = new BasicQuery("{}");
        Document sort = Document.parse("{title:1},{payment:-1}");
        query.setSortObject(sort);

        List<Order> orders = mongoTemplate.find(query, Order.class);
        orders.stream().forEach(System.out::println);
    }

    /**
     * 分页+排序
     */
    @Test
    public void testPaginationAndSort() {
//        Query query = new Query();
//        query.with(PageRequest.of(0, 3, Sort.by(Sort.Direction.DESC, "payment")));

        BasicQuery query = new BasicQuery("{}");
        Document sort = Document.parse("{title:1},{payment:-1}");
        query.setSortObject(sort);
        query.skip(0);
        query.limit(3);

        List<Order> orders = mongoTemplate.find(query, Order.class);
        orders.stream().forEach(System.out::println);
    }

    /**
     * 聚合
     * <p>
     * aggregate(TypedAggregation, Class)
     * <p>
     * TypedAggregation - 聚合逻辑
     * <p>
     * TypedAggregation.newAggregation(
     * Class, Aggregation...
     * );
     * <p>
     * AggregationResults - 聚合结果
     * results.getUniqueMappedResult(); - 返回聚合后的单行数据，如果聚合结果是多行，抛出异常。
     * results.getMappedResults(); - 返回聚合后的结果集合，是多行数据。（0-n行）
     */
    @Test
    public void testAgg() {
//        TypedAggregation<Order> typedAggregation =
//                TypedAggregation.newAggregation(Order.class,
//                        Aggregation.match(Criteria.where("payment").gt("100")),
//                        Aggregation.group("title")
//                                .sum("age").as("sum")
//                                .avg("age").as("avg")
//                                .max("age").as("max")
//                                .min("age").as("min")
//                        Aggregation.skip(1),
//                        Aggregation.limit(1)
//                );

        List<Document> list = new ArrayList<>();
        Document match = new Document("$match", Document.parse("{age:{$gt:1}}")); //匹配
        Document group = new Document("$group", Document.parse("{_id:\"$title\",sumAge:{$sum:\"$age\"}}")); //分组
        Document sort = new Document("$sort", Document.parse("{age:-1}"));
        list.add(match);
        list.add(group);
        list.add(sort);

        AggregateIterable<Document> aggregate = mongoTemplate.getCollection("order").aggregate(list);
        MongoCursor<Document> iterator = aggregate.iterator();
        while (iterator.hasNext()) {
            Document document = iterator.next();
            System.out.println(document.toJson());
        }
    }
}
