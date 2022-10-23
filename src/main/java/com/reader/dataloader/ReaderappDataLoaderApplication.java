package com.reader.dataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.reader.dataloader.author.Author;
import com.reader.dataloader.author.AuthorRepository;
import com.reader.dataloader.books.Book;
import com.reader.dataloader.books.BookRepository;
import com.reader.dataloader.configuration.DataStaxAstraProperties;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class ReaderappDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	@Value("${dataloader.location.author}")
	private String authorDumpPath;

	@Value("${dataloader.location.work}")
	private String workDumpPath;

	public static void main(String[] args) {
		SpringApplication.run(ReaderappDataLoaderApplication.class, args);
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

	@PostConstruct
	public void start() {
		initAuthors();
		initWorks();
	}

	private void initWorks() {
		Path path = Paths.get(workDumpPath);

		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				try {
					String bookString = line.substring(line.indexOf("{"));
					JsonObject bookJsonObj = JsonParser.parseString(bookString).getAsJsonObject();

					Book book = new Book();
					book.setName(bookJsonObj.get("title").toString());
					book.setId(bookJsonObj.get("key").toString().replace("/works/", ""));

					String desc = Optional.ofNullable(bookJsonObj.get("description")).map(JsonElement::getAsString)
							.orElse(null);

					Optional.ofNullable(desc).map(x -> JsonParser.parseString(x).getAsJsonObject())
							.ifPresent(x -> Optional.ofNullable(x.get("value"))
									.ifPresent(xy -> book.setDescription(xy.toString())));

					Optional.ofNullable(bookJsonObj.get("created")).map(JsonElement::getAsJsonObject)
							.ifPresent(x -> Optional.ofNullable(x.get("value")).ifPresent(xy -> book.setPublishedDate(
									LocalDate.parse(xy.getAsString().subSequence(0, xy.getAsString().indexOf("T"))))));

					List<String> coverIds = new ArrayList<String>();
					String cov = Optional.ofNullable(bookJsonObj.get("covers")).map(JsonElement::toString).orElse(null);
					Optional.ofNullable(cov)
							.ifPresent(x -> Optional.ofNullable(JsonParser.parseString(x).getAsJsonArray())
									.ifPresent(xy -> xy.forEach(z -> coverIds.add(z.getAsString()))));
					book.setCoverIds(coverIds);

					List<String> authorIds = new ArrayList<String>();
					String authJson = Optional.ofNullable(bookJsonObj.get("authors")).map(JsonElement::toString)
							.orElse(null);
					Optional.ofNullable(authJson)
							.ifPresent(x -> Optional.ofNullable(JsonParser.parseString(x).getAsJsonArray())
									.ifPresent(xy -> xy.forEach(z -> authorIds.add(JsonParser
											.parseString(z.getAsJsonObject().get("author").toString()).getAsJsonObject()
											.get("key").toString().replace("/authors/", "")))));
					book.setAuthorIds(authorIds);

					List<String> authorNames = new ArrayList<String>();
					for (String authorId : authorIds) {
						Author author = authorRepository.findById(authorId).get();
						authorNames.add(author.getName());
					}
					book.setAuthorNames(authorNames);

					bookRepository.save(book);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void initAuthors() {
		Path path = Paths.get(authorDumpPath);

		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				try {
					String authorString = line.substring(line.indexOf("{"));
					JsonObject authorJsonObj = JsonParser.parseString(authorString).getAsJsonObject();

					Author author = new Author();
					author.setId(authorJsonObj.get("key").toString().replace("/authors/", ""));
					author.setName(authorJsonObj.get("name").toString());
					author.setPersonalName(Optional.ofNullable(authorJsonObj.get("personal_name"))
							.map(JsonElement::toString).orElse(""));
					authorRepository.save(author);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			});

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
