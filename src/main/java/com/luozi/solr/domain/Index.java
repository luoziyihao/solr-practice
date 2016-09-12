package com.luozi.solr.domain;

import lombok.Data;
import org.apache.solr.client.solrj.beans.Field;

import java.awt.*;

/**
 * Created by luoziyihao on 9/11/16.
 */
@Data
public class Index {
    @Field
    private String id;
    @Field
    private String content;
}
