package persistencia;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import model.Cambrer;
import model.Restaurant;
import principal.Element;
import principal.GestioReservesExcepcio;

/**
 *
 * @author fta
 */
public class GestorJDBC implements ProveedorPersistencia {

    private Restaurant restaurant;

    private Connection conn; //Connexió a la base de dades

    public Restaurant getRestaurant() {
        return restaurant;
    }

    public void setRestaurant(Restaurant restaurant) {
        this.restaurant = restaurant;
    }

    /*
     PreparedStatement necessaris
     */

 /*
     * TODO
     *
     * Obtenir un restaurant
     * 
     * Sentència select de la taula restaurant
     * Columnes: totes
     * Files: totes les que el codi de restaurant sigui el donat per paràmetre
     *
     */
    private static String codiRestaurantSQL = ""
            + "SELECT * "
            + "FROM restaurant r "
            + "WHERE r.codi = ?";

    private PreparedStatement codiRestaurantSt;

    /*
     * TODO
     *
     * Inserir a restaurant
     * 
     * Sentència d'inserció de la taula restaurant
     * Els valors d'inserció són els que es donaran per paràmetre
     *
     */
    private static String insereixRestaurantSQL = ""
            + "INSERT INTO restaurant(codi, nom, adreca) "
            + " VALUES (?, ?, ?)";

    private PreparedStatement insereixRestaurantSt;

    /*
     * TODO
     *
     * Actualitzar restaurant
     * 
     * Sentència d'actualització de la taula restaurant
     * Files a actualitzar: les que es corresponguin amb el codi donat per paràmetre
     * Columnes a actualitzar: nom i adreca als valors donats per paràmetre
     *
     */
    private static String actualitzaRestaurantSQL = ""
            + " UPDATE restaurant "
            + " SET nom = ?, adreca = ? "
            + " WHERE codi = ? ";

    private PreparedStatement actualitzaRestaurantSt;

    /*
     * TODO
     *
     * Eliminar cambrers (donat el codi d'un restaurant)
     * 
     * Sentència d'eliminació de la taula cambrer
     * Files a eliminar: les que es corresponguin amb el codi del restaurant donat per paràmetre
     *
     */
    private static String eliminaCambrerSQL = ""
            + " DELETE FROM cambrer "
            + " WHERE restaurant = ?";

    private PreparedStatement eliminaCambrerSt;

    /*
     * TODO
     *
     * Inserir a cambrer
     * 
     * Sentència d'inserció de la taula cambrer
     * Els valors d'inserció són els que es donaran per paràmetre
     *
     */
    private static String insereixCambrerSQL = ""
            + "INSERT INTO cambrer(codi, nom, telefon, torn, actiu, restaurant) "
            + " VALUES (?, ?, ?, ?, ?, ?)";

    private PreparedStatement insereixCambrerSt;

    /*
     Seleccionar cambrer donat un restaurant
     */
 /*
     * TODO
     *
     * Seleccionar els cambrers d'un restaurant
     * 
     * Sentència select de la taula cambrer
     * Columnes: totes
     * Files: totes les que el codi de restaurant sigui el donat per paràmetre
     *
     */
    private static String selCambrerSQL = ""
            + " SELECT * "
            + " FROM cambrer "
            + " WHERE restaurant = ?";

    private PreparedStatement selCambrerSt;

    /*
     *TODO
     * 
     *Paràmetres: cap
     *
     *Acció:
     *  - Heu d'establir la connexio JDBC amb la base de dades EAC111718S2
     *  - Heu de crear els objectes PrepareStatement declarats com a atributs d'aquesta classe
     *    amb els respectius SQL declarats com a atributs just sobre cadascun d'ells.
     *  - Heu de fer el catch de les possibles excepcions (en aquest mètode no llançarem GestioReservesExcepcio,
     *    simplement, mostreu el missatge a consola de l'excepció capturada)
     *
     *Retorn: cap
     *
     */
    public void estableixConnexio() throws SQLException {

        String urlBaseDades = "jdbc:derby://localhost:1527/EAC111718S2";
        String usuari = "root";
        String contrasenya = "root123";

        try {
            conn = DriverManager.getConnection(urlBaseDades, usuari, contrasenya);
            codiRestaurantSt = conn.prepareStatement(codiRestaurantSQL);
            insereixRestaurantSt = conn.prepareStatement(insereixRestaurantSQL);
            actualitzaRestaurantSt = conn.prepareStatement(actualitzaRestaurantSQL);
            eliminaCambrerSt = conn.prepareStatement(eliminaCambrerSQL);
            insereixCambrerSt = conn.prepareStatement(insereixCambrerSQL);
            selCambrerSt = conn.prepareStatement(selCambrerSQL);
        } catch (SQLException e) {
            conn = null;
            System.out.println(e.getMessage());
            throw e;
        }
    }

    /**
     * Tanca la connexió i posa la referència a la connexió a null.
     *
     * @throws SQLException
     */
    public void tancaConnexio() throws SQLException {
        try {
            conn.close();
        } finally {
            conn = null;
        }
    }

    /*
     *TODO
     * 
     *Paràmetres: el nom del fitxer i el restaurant a desar
     *
     *Acció:
     *  - Heu de desar el restaurant sobre la base de dades:
     *  - El restaurant s'ha de desar a la taula restaurants (nomFitxer conté el codi del restaurant)
     *  - Cada cambrer del restaurant, s'ha de desar com registre de la taula cambrer
     *  - Heu de tenir en compte que si el restaurant ja existia amb aquest codi, llavors heu de fer el següent:
     *     - Actualitzar el registre restaurant ja existent
     *     - Eliminar tots els cambrers d'aquest restaurant de la taula cambrer i després inserir els nous com si hagués estat
     *       un restaurant nou.
     *  - Si al fer qualsevol operació es dona una excepció, llavors heu de llançar l'excepció GestioReservesExcepcio amb codi "GestorJDBC.desar"
     *
     *Retorn: cap
     *
     */
    @Override
    public void desarRestaurant(String nomFitxer, Restaurant restaurant) throws GestioReservesExcepcio {

        try {
            if (conn == null) {
                estableixConnexio();
            }

            codiRestaurantSt.setInt(1, restaurant.getCodi());
            ResultSet registresRestaurant = codiRestaurantSt.executeQuery();

            // Només hi pooden haver 0 o 1 resultats
            if (registresRestaurant.next()) { //Existeix el restaurant

                //Actualitzar restaurant
                actualitzaRestaurantSt.setInt(1, restaurant.getCodi());
                actualitzaRestaurantSt.setString(2, restaurant.getNom());
                actualitzaRestaurantSt.setString(3, restaurant.getAdreca());
                actualitzaRestaurantSt.executeUpdate();

                //Eliminem cambrers
                eliminaCambrerSt.setInt(1, restaurant.getCodi());
                eliminaCambrerSt.executeUpdate();

            } else { //No existeix el restaurant

                //Insercio restaurant
                insereixRestaurantSt.setInt(1, restaurant.getCodi());
                insereixRestaurantSt.setString(2, restaurant.getNom());
                insereixRestaurantSt.setString(3, restaurant.getAdreca());
                insereixRestaurantSt.executeUpdate();

            }

            //Insercio cambrers del restaurant
            for (Element element : restaurant.getElements()) {
                if (element != null && element instanceof Cambrer) {
                    insereixCambrerSt.setString(1, ((Cambrer) element).getCodi());
                    insereixCambrerSt.setString(2, ((Cambrer) element).getNom());
                    insereixCambrerSt.setString(3, ((Cambrer) element).getTelefon());
                    insereixCambrerSt.setString(4, ((Cambrer) element).getTorn());
                    insereixCambrerSt.setBoolean(5, ((Cambrer) element).getActiu());
                    insereixCambrerSt.setInt(6, restaurant.getCodi());
                    insereixCambrerSt.executeUpdate();
                }
            }
            tancaConnexio();

        } catch (SQLException ex) {
            throw new GestioReservesExcepcio("GestorJDBC.desar");
        }
    }

    /*
     *TODO
     * 
     *Paràmetres: el nom del fitxer del restaurant
     *
     *Acció:
     *  - Heu de carregar el restaurant des de la base de dades (nomFitxer és el codi del restaurant)
     *  - Per fer això, heu de cercar el registre restaurant de la taula amb codi = nomFitxer
     *  - A més, heu d'afegir els cambrers al restaurant a partir de la taula cambrer
     *  - Si al fer qualsevol operació es dona una excepció, llavors heu de llançar l'excepció GestioReservesExcepcio 
     *    amb codi "GestorJDBC.carregar"
     *  - Si el nomFitxer donat no existeix a la taula restaurant (és a dir, el codi = nomFitxer no existeix), llavors
     *    heu de llançar l'excepció GestioReservesExcepcio amb codi "GestorJDBC.noexist"
     *
     *Retorn: cap
     *
     */
    @Override
    public void carregarRestaurant(String nomFitxer) throws GestioReservesExcepcio {

        try {

            if (conn == null) {
                estableixConnexio();
            }

            codiRestaurantSt.setInt(1, Integer.parseInt(nomFitxer));
            ResultSet registresRestaurants = codiRestaurantSt.executeQuery();

            // Només hi pooden haver 0 o 1 resultats
            if (registresRestaurants.next()) {

                restaurant = new Restaurant(registresRestaurants.getString("nom"), registresRestaurants.getString("adreca"), registresRestaurants.getInt("codi"));

                //Seleccionem els cambrers de la taula cambrer i els afegim al restaurant
                selCambrerSt.setInt(1, restaurant.getCodi());

                ResultSet registresCambrers = selCambrerSt.executeQuery();

                while (registresCambrers.next()) {
                    restaurant.addCambrer(new Cambrer(registresCambrers.getString("codi"), registresCambrers.getString("nom"), registresCambrers.getString("telefon"), registresCambrers.getString("torn"), registresCambrers.getBoolean("actiu")));
                }

            } else {
                throw new GestioReservesExcepcio("GestorJDBC.noExisteix");
            }
            tancaConnexio();
            
        } catch (SQLException ex) {
            throw new GestioReservesExcepcio("GestorJDBC.carregar");
        }
    }

}
