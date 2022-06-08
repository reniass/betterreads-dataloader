package com.reinke.betterreadsdataloader;

import com.reinke.betterreadsdataloader.author.Author;
import com.reinke.betterreadsdataloader.author.AuthorRepository;
import com.reinke.betterreadsdataloader.book.Book;
import com.reinke.betterreadsdataloader.book.BookRepository;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@SpringBootApplication
public class BetterreadsDataloaderApplication {


	@Autowired
	private AuthorRepository authorRepository;

	@Autowired
	private BookRepository bookRepository;

	@Value(value = "${datadump.location.authors}")
	private String authorsDumpLocation;
	@Value(value = "${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataloaderApplication.class, args);
	}

	public void initAuthors() {
		Path path = Paths.get(authorsDumpLocation);
		try(Stream<String> lines =  Files.lines(path)) {

			lines.forEach(line -> {
				try {
					JSONObject jsonObject = new JSONObject(line.substring(line.indexOf("{")));

					Author author = new Author();
					author.setId(jsonObject.optString("key").replace("/authors/", ""));
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("personal_name"));

					authorRepository.save(author);


				} catch (Exception e) {
					e.printStackTrace();
				}

			});

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void initWorks() {
		Path path = Paths.get(worksDumpLocation);
		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try(Stream<String> lines =  Files.lines(path)) {

			lines.forEach(line -> {
				try {
					JSONObject jsonObject = new JSONObject(line.substring(line.indexOf("{")));

					Book book = new Book();
					book.setId(jsonObject.optString("key").replace("/works/", ""));
					book.setTitle(jsonObject.optString("title"));

					JSONObject jsonObjectDescription = jsonObject.optJSONObject("description");
					if (jsonObjectDescription != null) {
						book.setDescription(jsonObjectDescription.optString("value"));
					}

					JSONArray jsonArrayCovers = jsonObject.optJSONArray("covers");
					if (jsonArrayCovers != null) {
						List<String> coverIds= new ArrayList<>();
						for (int i=0; i<jsonArrayCovers.length(); i++) {
							coverIds.add(jsonArrayCovers.getString(i));
						}
						book.setCoverId(coverIds);
					}

					JSONArray jsonArrayAuthors = jsonObject.optJSONArray("authors");
					if (jsonArrayAuthors != null) {
						List<String> authorsIds = new ArrayList<>();
						List<String> authorsNames = new ArrayList<>();
						for (int i=0; i < jsonArrayAuthors.length(); i++ ) {
							JSONObject jsonObjectAuthor = jsonArrayAuthors.getJSONObject(i);
							String authorId = jsonObjectAuthor.optJSONObject("author").optString("key").replace("/authors/", "");
							authorsIds.add(authorId);

							Optional<Author> optionalAuthor = authorRepository.findById(authorId);
							if (optionalAuthor.isPresent()) {
								String authorName = optionalAuthor.get().getName();
								authorsNames.add(authorName);
							}

						}
						book.setAuthorsIds(authorsIds);
						book.setAuthorsName(authorsNames);
					}

					// correct format of local date
					JSONObject jsonObjectCreated = jsonObject.optJSONObject("created");
					if (jsonObjectCreated != null) {
						book.setPublicationDate(LocalDate.parse(jsonObjectCreated.optString("value"), dateFormat));
					}

					bookRepository.save(book);

				} catch (Exception e) {
					e.printStackTrace();
				}

			});

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@PostConstruct
	public void start() {
		initAuthors();
		initWorks();
	}
}
