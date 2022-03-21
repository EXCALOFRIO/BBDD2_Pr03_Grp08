import java.sql.*;

/**
 * @author Javier Linares Castrillón
 * @see <a href="https://github.com/Romuloo">Git Hub</a>
 */
public class MysqlCon{

    private static String con_1_url = "jdbc:mysql://localhost:3306/world";
    private static String con_2_url = "jdbc:mysql://localhost:3306/sakila";
    private static String user = "root";
    private static String pss = "root";

    //nº de ciudades
    private static String conteo_world = "SELECT count(*) from city";
    //País con más población
    private static String max_population = "SELECT name from country where population = (SELECT MAX(population) as \"Max Population\" from country);";
    // Todos los registros
    private static String all_languages = "SELECT * from countrylanguage";
    // Algunos registros - países hispanohablantes
    private static String spanish_speakers = "SELECT name FROM country , countrylanguage WHERE country.code = countrylanguage.countrycode" +
            " and countrylanguage.language = \"Spanish\";";
    //Datos de los países, su continente y su forma de gobierno de los paises que tienen un lenguaje oficial,
    // y los agrupamos por el nombre del pais.
    private static String consulta = "SELECT city.CountryCode,country.Name, city.District, countrylanguage.Language,countrylanguage." +
            "Percentage,(city.Population*countrylanguage.Percentage/100) AS NumeroPersonasHablantes\n" +
            "FROM city \n" +
            "\tINNER JOIN countrylanguage ON countrylanguage.CountryCode = city.CountryCode \n" +
            "\tINNER JOIN country ON country.Code = city.CountryCode\n" +
            "WHERE city.Population > 3276207;";
    //Datos de las ciudades con una población mayor a 3276207 y muestra el numero de habitantes dentro
    // de un pais que habla un idioma
    private static String consulta_2 = "SELECT distinct country.Name, country.Continent, country.GovernmentForm FROM country   " +
            "  INNER JOIN city ON city.CountryCode = country.Code     INNER JOIN countrylanguage ON countrylanguage.CountryCode = city.CountryCode     " +
            "WHERE countrylanguage.IsOfficial =\"T\";";

    //nº de actores
    private static String conteo_sakila = "SELECT count(*) FROM actor;";
    // nº de países en country con un nombre que empieza por A
    private static String conteo_paises_por_A = "SELECT COUNT(*) as num_countries FROM country WHERE upper(country) LIKE 'A%';";
    // Esta consulta devuelve todos los registros de la tabla payment.
    private static String all_payments = "select * from payment;";
    // Toda la información en la tabla Address de los distritos California y Buenos Aires, o donde el CP o el Teléfono termine en 5.
    private static String direcciones = "SELECT * FROM sakila.address where district='California' or district='Buenos Aires' or postal_code like '%5'" +
            " or phone like '%5';";
    // Esta consulta devuelve el cliente que más películas ha alquilado.
    private static String mejor_cliente = "Select CONCAT(customer.first_name,' ',customer.last_name) as Cliente, count(rental.customer_id) as Total " +
            "from rental inner join customer on rental.customer_id=customer.customer_id group by rental.customer_id;";
    // Datos de los clientes con un retraso en la devolución
    private static String clientes_con_retraso = "SELECT CONCAT(customer.last_name, ', ', customer.first_name) AS customer, address.phone," +
            " film.title,customer.email,rental.return_date, payment.payment_date, payment.amount FROM rental\n" +
            "INNER JOIN customer ON rental.customer_id = customer.customer_id\n" +
            "INNER JOIN address ON customer.address_id = address.address_id\n" +
            "INNER JOIN inventory ON rental.inventory_id = inventory.inventory_id\n" +
            "INNER JOIN film ON inventory.film_id = film.film_id\n" +
            "INNER JOIN payment ON payment.rental_id = rental.rental_id\n" +
            "WHERE rental.return_date IS NULL\n" +
            "AND rental_date + INTERVAL film.rental_duration DAY < CURRENT_DATE();";



    public static void main(String args[]){
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection con_1 =DriverManager.getConnection(
                    con_1_url,user, pss);

            System.out.println("Consultas a la base de datos World:");
            lanzarConsulta("Número de ciudades", conteo_world, con_1.createStatement());
            lanzarConsulta("País con más población", max_population, con_1.createStatement());
            lanzarConsulta("Todos los lenguages", all_languages, con_1.createStatement());
            lanzarConsulta("Países hispano hablantes", spanish_speakers, con_1.createStatement());
            lanzarConsulta("Personas que hablan el lenguaje principal", consulta, con_1.createStatement());
            lanzarConsulta("Ciudades con más de 3276207", consulta_2, con_1.createStatement());
            con_1.close();

            Connection con_2 =DriverManager.getConnection(
                    con_2_url,user, pss);
            System.out.println("Consultas a la base de datos Sakila:");
            lanzarConsulta("Número de actores", conteo_sakila, con_2.createStatement());
            lanzarConsulta("Número de países que empiezan por A", conteo_paises_por_A, con_2.createStatement());
            lanzarConsulta("Registros en la tabla Payment", all_payments, con_2.createStatement());
            lanzarConsulta("Direcciones en Buenos Aires o California", direcciones, con_2.createStatement());
            lanzarConsulta("Cliente que más películas ha comprado", conteo_sakila, con_2.createStatement());
            lanzarConsulta("Datos de los clientes con un retraso en la devolución", conteo_sakila, con_2.createStatement());
            con_2.close();

        }catch(Exception e){ System.out.println(e);}
    }


    private static void lanzarConsulta(String nombre, String query, Statement stm){
        try {
            System.out.println();
            System.out.println("Coonsulta: " + nombre +".");
            long startTime = System.currentTimeMillis();
            stm.executeQuery(query);
            long endTime = System.currentTimeMillis() - startTime;
            System.out.println("La consulta de datos ha tardado " + endTime + " ms.");
            long startTime2 = System.currentTimeMillis();
            stm.executeQuery(query);
            long endTime2 = System.currentTimeMillis() - startTime2;
            System.out.println("La segunda ejecución de la consulta ha tardado " + endTime2 + " ms.");
            System.out.println();

        }catch (SQLException sqlException){
            System.err.println(sqlException);
        }

    }
}  