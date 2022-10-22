package com.reader.dataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import com.reader.dataloader.configuration.DataStaxAstraProperties;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class ReaderappDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

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
		System.out.println("work location :: " + workDumpPath);

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
