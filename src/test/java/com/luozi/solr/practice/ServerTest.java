package com.luozi.solr.practice;

import com.luozi.solr.domain.Index;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * <b>function:</b> Server TestCase
 * 增删查看
 * 尤其是  query
 * params.set*
 * 分片查询
 * @author hoojo
 * @version 1.0
 * @createDate 2011-10-19 下午01:49:07
 * @file ServerTest.java
 * @package com.hoo.test
 * @project SolrExample
 * @blog http://blog.csdn.net/IBM_hoojo
 * @email hoojo_@126.com
 */
public class ServerTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private SolrServer server;
    private CommonsHttpSolrServer httpServer;

    private static final String DEFAULT_URL = "http://localhost:8983/solr/";

    @Before
    public void init() {
        logger.debug("runok");
        try {

            httpServer = new CommonsHttpSolrServer(DEFAULT_URL);
            server = new CommonsHttpSolrServer(DEFAULT_URL);
            /*3、 Server的有关配置选项参数，server是CommonsHttpSolrServer的实例*/

            httpServer.setSoTimeout(1000); // socket read timeout
            httpServer.setConnectionTimeout(100);
            httpServer.setDefaultMaxConnectionsPerHost(100);
            httpServer.setMaxTotalConnections(100);
            httpServer.setFollowRedirects(false); // defaults to false
            // allowCompression defaults to false.
            // Server side must support gzip or deflate for this to have any effect.
            httpServer.setAllowCompression(true);
            httpServer.setMaxRetries(1); // defaults to 0.  > 1 not recommended.

            //sorlr J 目前使用二进制的格式作为默认的格式。对于solr1.2的用户通过显示的设置才能使用XML格式。
//            httpServer.setParser(new XMLResponseParser());

            //二进制流输出格式
            //server.setRequestWriter(new BinaryRequestWriter());

        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void destory() {
        server = null;
        httpServer = null;
        System.runFinalization();
        System.gc();
    }

    public final void debug(Object o) {
        logger.debug("{}", o);
    }

    /**
     * <b>function:</b> 测试是否创建server对象成功
     *
     * @author hoojo
     * @createDate 2011-10-21 上午09:48:18
     */
    @Ignore
    @Test
    public void server() {
        debug(server);
        debug(httpServer);
        query("select");
    }

    /**
     * <b>function:</b> 根据query参数查询索引
     *
     * @param query
     * @author hoojo
     * @createDate 2011-10-21 上午10:06:39
     */
    public void query(String query) {
        debug(query);
        SolrParams params = new SolrQuery(query);

        try {
            QueryResponse response = server.query(params);

            SolrDocumentList list = response.getResults();
            for (int i = 0; i < list.size(); i++) {
                debug(list.get(i));
            }
        } catch (SolrServerException e) {
            e.printStackTrace();
        }
    }

    /* 4、 利用SolrJ完成Index Document的添加操作 */

    /**
     * <b>function:</b> 添加doc文档
     *
     * @author hoojo
     * @createDate 2011-10-21 上午09:49:10
     */
    @Test
    public void addDoc() {
        //创建doc文档
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "22222222222222222222");
        doc.addField("segment", "segment");
        doc.addField("text", "Solr Input Document");

        try {
            //添加一个doc文档
            UpdateResponse response = server.add(doc);
            debug(server.commit());//commit后才保存到索引库
            debug(response);
            debug("query time：" + response.getQTime());
            debug("Elapsed Time：" + response.getElapsedTime());
            debug("status：" + response.getStatus());
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*5、 利用SolrJ添加多个Document，即添加文档集合*/

/**
 * <b>function:</b> 添加docs文档集合
 * @author hoojo
 * @createDate 2011-10-21 上午09:55:01
 */
    @Test
    public void addDocs() {
        Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", 2);
        doc.addField("segment", "Solr Input Documents 2");

        docs.add(doc);

        doc = new SolrInputDocument();
        doc.addField("id", 3);
        doc.addField("segment", "Solr Input Documents 3");


        docs.add(doc);

        try {
            //add docs
            UpdateResponse response = server.add(docs);
            //commit后才保存到索引库
            debug(server.commit());
            debug(response);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        query("id:3");
    }

    /*添加Bean完成doc添加操作*/

/**
 * <b>function:</b> 添加JavaEntity Bean
 * @author hoojo
 * @createDate 2011-10-21 上午09:55:37
 */
    @Test
    public void addBean() {
        //Index需要添加相关的Annotation注解，便于告诉solr哪些属性参与到index中
        Index index = new Index();
        index.setId("add bean 4");
        index.setContent("add bean index");

        try {
            //添加Index Bean到索引库
            UpdateResponse response = server.addBean(index);
            debug(server.commit());//commit后才保存到索引库
            debug(response);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        query("id:4");
    }

    /*7、 添加Bean集合*/

/**
 * <b>function:</b> 添加Entity Bean集合到索引库
 * @author hoojo
 * @createDate 2011-10-21 上午10:00:55
 */
    @Test
    public void addBeans() {
        Index index = new Index();
        index.setId("6");
        index.setContent("add beans index 6");

        List<Index> indexs = new ArrayList<Index>();
        indexs.add(index);

        index = new Index();
        index.setId("5");
        index.setContent("add beans index 5");
        indexs.add(index);
        try {
            //添加索引库
            UpdateResponse response = server.addBeans(indexs);
            debug(server.commit());//commit后才保存到索引库
            debug(response);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        query("beans");
    }

    /*8、 删除索引Document*/

/**
 * <b>function:</b> 删除索引操作
 * @author hoojo
 * @createDate 2011-10-21 上午10:04:28
 */
    @Test
    public void remove() {
        try {
            //删除id为1的索引
            server.deleteById("1");
            server.commit();
            query("id:1");

            //根据id集合，删除多个索引
            List<String> ids = new ArrayList<String>();
            ids.add("2");
            ids.add("3");
            server.deleteById(ids);
            server.commit(true, true);
            query("id:3 id:2");

            //删除查询到的索引信息
            server.deleteByQuery("id:4 id:6");
            server.commit(true, true);
            query("id:4");
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*9、 查询索引*/

/**
 * <b>function:</b> 查询所有索引信息
 * @author hoojo
 * @createDate 2011-10-21 上午10:05:38
 */
    @Test
    public void queryAll() {
        ModifiableSolrParams params = new ModifiableSolrParams();
        // 查询关键词，*:*代表所有属性、所有值，即所有index
        params.set("q", "*:*");
        // 分页，start=0就是从0开始，，rows=5当前返回5条记录，第二页就是变化start这个值为5就可以了。
        params.set("start", 0);
//        params.set("rows", Integer.MAX_VALUE);
        params.set("rows", 10);

        // 排序，，如果按照id 排序，，那么将score desc 改成 id desc(or asc)
        params.set("sort", "score desc");

        // 返回信息 * 为全部 这里是全部加上score，如果不加下面就不能使用score
        params.set("fl", "*,score");

        try {
            QueryResponse response = server.query(params);

            SolrDocumentList list = response.getResults();
            for (int i = 0; i < list.size(); i++) {
                debug(list.get(i));
            }
        } catch (SolrServerException e) {
            e.printStackTrace();
        }
    }

    /*10、 其他和Server有关方法*/

/**
 * <b>function:</b> 其他server相关方法测试
 * @author hoojo
 * @createDate 2011-10-21 上午10:02:03
 */
    @Test
    public void otherMethod() {
        debug(server.getBinder());
        try {
            debug(server.optimize());//合并索引文件，可以优化索引、提供性能，但需要一定的时间
            debug(server.ping());//ping服务器是否连接成功

            Index index = new Index();
            index.setId("299");
            index.setContent("add bean index199");

            UpdateResponse response = server.addBean(index);
            debug("response: " + response);

            queryAll();
            //回滚掉之前的操作，rollback addBean operation
            debug("rollback: " + server.rollback());
            //提交操作，提交后无法回滚之前操作；发现addBean没有成功添加索引
            debug("commit: " + server.commit());
            queryAll();
            query("id:299");
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* 11、 文档查询 */

/**
 * <b>function:</b> query 基本用法测试
 * @author hoojo
 * @createDate 2011-10-20 下午04:44:28
 */
    @Test
    public void queryCase() {
        //AND 并且
        SolrQuery params = new SolrQuery("content:apple AND title:inc");

        //OR 或者
        params.setQuery("content:apple OR title:inc");
        //空格 等同于 OR
        params.setQuery("content:apple title:inc");

        //params.setQuery("name:solr - manu:inc");
        //params.setQuery("name:server + manu:dell");

        //查询content包含solr apple
        params.setQuery("content:solr,apple");
        //content不包含inc
        params.setQuery("content:solr,apple NOT content:inc");

        //50 <= boost <= 200
        params.setQuery("boost:[50 TO 200]");
        params.setQuery("boost:[5 TO 6]");
        //params.setQuery("price:[50 TO 200] - popularity:[5 TO 6]");
        //params.setQuery("price:[50 TO 200] + popularity:[5 TO 6]");

        //50 <= boost <= 200 AND 5 <= popularity <= 6
        params.setQuery("boost:[50 TO 200] AND boost:[5 TO 6]");
        params.setQuery("boost:[50 TO 200] OR boost:[5 TO 6]");

        //过滤器查询，可以提高性能 filter 类似多个条件组合，如and
        //params.addFilterQuery("id:VA902B");
        //params.addFilterQuery("price:[50 TO 200]");
        //params.addFilterQuery("popularity:[* TO 5]");
        //params.addFilterQuery("weight:*");
        //0 < popularity < 6  没有等于
        //params.addFilterQuery("popularity:{0 TO 6}");

        //排序
        params.addSortField("id", SolrQuery.ORDER.asc);

        //分页：start开始页，rows每页显示记录条数
        //params.add("start", "0");
        //params.add("rows", "200");
        //params.setStart(0);
        //params.setRows(200);

        //设置高亮
        params.setHighlight(true); // 开启高亮组件
        params.addHighlightField("content");// 高亮字段
        params.setHighlightSimplePre("<font color='red'>");//标记，高亮关键字前缀
        params.setHighlightSimplePost("</font>");//后缀
        params.setHighlightSnippets(1);//结果分片数，默认为1
        params.setHighlightFragsize(1000);//每个分片的最大长度，默认为100

        //分片信息
        params.setFacet(true)
                .setFacetMinCount(1)
                .setFacetLimit(5)//段
                .addFacetField("title")//分片字段
                .addFacetField("ip");

        //params.setQueryType("");

        try {
            QueryResponse response = server.query(params);

        /*List<Index> indexs = response.getBeans(Index.class);
        for (int i = 0; i < indexs.size(); i++) {
            debug(indexs.get(i));
        }*/

            //输出查询结果集
            SolrDocumentList list = response.getResults();
            debug("query result nums: " + list.getNumFound());
            for (int i = 0; i < list.size(); i++) {
                debug(list.get(i));
            }

            //输出分片信息
            List<FacetField> facets = response.getFacetFields();
            for (FacetField facet : facets) {
                debug(facet);
                List<FacetField.Count> facetCounts = facet.getValues();
                Assert.assertNull(facetCounts);
//                for (FacetField.Count count : facetCounts) {
//                    System.out.println(count.getName() + ": " + count.getCount());
//                }
            }
        } catch (SolrServerException e) {
            e.printStackTrace();
        }
    }

    /*12、 分片查询、统计*/

/**
 * <b>function:</b> 分片查询， 可以统计关键字及出现的次数、或是做自动补全提示
 * @author hoojo
 * @createDate 2011-10-20 下午04:54:25
 */
    @Test
    public void facetQueryCase() {
        SolrQuery params = new SolrQuery("*:*");

        //排序
        params.addSortField("id", SolrQuery.ORDER.asc);

        params.setStart(0);
        params.setRows(200);

        //Facet为solr中的层次分类查询
        //分片信息
        params.setFacet(true)
                .setQuery("*:*")
                .setFacetMinCount(1)
                .setFacetLimit(5)//段
                //.setFacetPrefix("electronics", "cat")
                .setFacetPrefix("add")//查询manu、name中关键字前缀是cor的
                .addFacetField("id")
                .addFacetField("content");//分片字段

        try {
            QueryResponse response = server.query(params);

            //输出查询结果集
            SolrDocumentList list = response.getResults();
            debug("Query result nums: " + list.getNumFound());

            for (int i = 0; i < list.size(); i++) {
                debug(list.get(i));
            }

            debug("All facet filed result: ");
            //输出分片信息
            List<FacetField> facets = response.getFacetFields();
            for (FacetField facet : facets) {
                debug(facet);
                List<FacetField.Count> facetCounts = facet.getValues();
                for (FacetField.Count count : facetCounts) {
                    //关键字 - 出现次数
                    debug(count.getName() + ": " + count.getCount());
                }
            }

            debug("Search facet [content] filed result: ");
            //输出分片信息
            FacetField facetField = response.getFacetField("content");
            List<FacetField.Count> facetFields = facetField.getValues();
            for (FacetField.Count count : facetFields) {
                //关键字 - 出现次数
                debug(count.getName() + ": " + count.getCount());
            }
        } catch (SolrServerException e) {
            e.printStackTrace();
        }
    }

}
