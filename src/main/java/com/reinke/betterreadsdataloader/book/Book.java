package com.reinke.betterreadsdataloader.book;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import java.time.LocalDate;
import java.util.List;

@Table(value = "books_by_id")
public class Book {

    @Id
    @PrimaryKeyColumn(name = "book_id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
    private String id;

    @Column("title")
    @CassandraType(type = CassandraType.Name.TEXT)
    private String title;

    @Column("description")
    @CassandraType(type = CassandraType.Name.TEXT)
    private String description;

    @Column("cover_ids")
    @CassandraType(type = CassandraType.Name.LIST, typeArguments = CassandraType.Name.TEXT)
    private List<String> coverId;

    @Column("authors_ids")
    @CassandraType(type = CassandraType.Name.LIST, typeArguments = CassandraType.Name.TEXT)
    private List<String> authorsIds;

    @Column("authors_name")
    @CassandraType(type = CassandraType.Name.LIST, typeArguments = CassandraType.Name.TEXT)
    private List<String> authorsName;

    @Column("publication_date")
    @CassandraType(type = CassandraType.Name.DATE)
    private LocalDate publicationDate;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<String> getCoverId() {
        return coverId;
    }

    public void setCoverId(List<String> coverId) {
        this.coverId = coverId;
    }

    public List<String> getAuthorsIds() {
        return authorsIds;
    }

    public void setAuthorsIds(List<String> authorsIds) {
        this.authorsIds = authorsIds;
    }

    public List<String> getAuthorsName() {
        return authorsName;
    }

    public void setAuthorsName(List<String> authorsName) {
        this.authorsName = authorsName;
    }

    public LocalDate getPublicationDate() {
        return publicationDate;
    }

    public void setPublicationDate(LocalDate publicationDate) {
        this.publicationDate = publicationDate;
    }
}
