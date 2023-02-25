package pl.dawidkaszuba.betterreadsdataloader;

import connection.DataStaxAstraProperties;
import jakarta.annotation.PostConstruct;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import pl.dawidkaszuba.betterreadsdataloader.author.Author;
import pl.dawidkaszuba.betterreadsdataloader.author.AuthorRepository;
import pl.dawidkaszuba.betterreadsdataloader.book.Book;
import pl.dawidkaszuba.betterreadsdataloader.book.BookRepository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterReadsDataLoaderApplication {

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    @Autowired
    private AuthorRepository authorRepository;

    @Autowired
    private BookRepository bookRepository;

    public static void main(String[] args) {
        SpringApplication.run(BetterReadsDataLoaderApplication.class, args);
    }

    private void initAuthors() {
        Path path = Paths.get(authorDumpLocation);
        try (Stream<String> lines = Files.lines(path)) {

            lines.limit(10).forEach(line -> {
                String jsonString = line.substring(line.indexOf("{"));
                JSONObject jsonObject = new JSONObject(jsonString);
                Author author = new Author();
                author.setName(jsonObject.optString("name"));
                author.setPersonalName(jsonObject.optString("personal_name"));
                author.setId(jsonObject.optString("key").replace("/authors/", ""));
                authorRepository.save(author);
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initWorks() {

        Path path = Paths.get(worksDumpLocation);

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        try (Stream<String> lines = Files.lines(path)) {
            lines.forEach(line -> {
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                    Book book = new Book();
                    book.setId(jsonObject.optString("key").replace("/works/", ""));
                    book.setName(jsonObject.optString("title"));
                    JSONObject descriptionObj = jsonObject.optJSONObject("description");
                    if (descriptionObj != null) {
                        book.setDescription(descriptionObj.optString("value"));
                    }
                    JSONObject publishedObject = jsonObject.optJSONObject("created");
                    if (publishedObject != null) {
                        String dateStr = publishedObject.getString("value");
                        book.setPublishedDate(LocalDate.parse(dateStr, dateTimeFormatter));
                    }
                    JSONArray coversJsonArr = jsonObject.optJSONArray("covers");
                    if (coversJsonArr != null) {
                        List<String> coverIds = new ArrayList<>();
                        for (int i = 0; i < coversJsonArr.length(); i++) {
                            coverIds.add(String.valueOf(coversJsonArr.getInt(i)));
                        }
                        book.setCoverIds(coverIds);
                    }
                    JSONArray authorJsonArr = jsonObject.optJSONArray("authors");
                    if (authorJsonArr != null) {
                        List<String> authorIds = new ArrayList<>();
                        for (int i = 0; i < authorJsonArr.length(); i++) {
                            String authorId = authorJsonArr.getJSONObject(i).getJSONObject("author").getString("key").replace("/authors/", "");
                            authorIds.add(authorId);
                        }
                        book.setAuthorIds(authorIds);
                        List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id)).map(optionalAuthor -> {
                            System.out.println("optional author:  " + optionalAuthor);
                            if (optionalAuthor.isPresent()) {
                                return optionalAuthor.get().getName();
                            } else {
                                return "Unknown author";
                            }
                        }).collect(Collectors.toList());
                        book.setAuthorNames(authorNames);
                    }
                    System.out.println("Saving book " + book.getName() + "...");
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
        //initAuthors();
        initWorks();
    }

    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }

}
