package persistencia;

import java.util.List;
import model.Restaurant;
import principal.GestioReservesExcepcio;

import com.db4o.Db4oEmbedded;
import com.db4o.ObjectContainer;
import com.db4o.config.EmbeddedConfiguration;
import com.db4o.query.Predicate;

/**
 *
 * @author fta
 */
public class GestorDB4O implements ProveedorPersistencia {

    private ObjectContainer db;
    private Restaurant restaurant;

    public Restaurant getRestaurant() {
        return restaurant;
    }

    public void setRestaurant(Restaurant restaurant) {
        this.restaurant = restaurant;
    }

    /*
     *TODO
     * 
     *Paràmetres: cap
     *
     *Acció:
     *  - Heu de crear / obrir la base de dades "EAC111718S2.db4o"
     *  - Aquesta base de dades ha de permetre que Restaurant s'actualitzi en cascada.
     *
     *Retorn: cap
     *
     */
 
    public void estableixConnexio() {
        
        EmbeddedConfiguration config = Db4oEmbedded.newConfiguration();
        config.common().objectClass(Restaurant.class).cascadeOnUpdate(true);
        db = Db4oEmbedded.openFile(config, "EAC111718S2.db4o");
    }

    public void tancaConnexio() {
        db.close();
    }

    /*
     *TODO
     * 
     *Paràmetres: el nom del fitxer i el restaurant a desar
     *
     *Acció:
     *  - Estableixeu la connexio i al final tanqueu-la
     *  - Heu de desar l'objecte Restaurant passat per paràmetre sobre la base de dades 
     *    (heu d'inserir si no existia ja a la base de dades, o actualitzar en altre cas)
     *  - S'ha de fer la consulta de l'existència amb Predicate
     *
     *Retorn: cap
     *
     */
    @Override
    public void desarRestaurant(String nomFitxer, Restaurant pRestaurant) throws GestioReservesExcepcio {

        estableixConnexio();
        final int codi = Integer.parseInt(nomFitxer);
        
        List<Restaurant> restaurants = db.query(new Predicate<Restaurant>() {
            public boolean match(Restaurant unRestaurant) {
                return unRestaurant.getCodi()==codi;
            }
        });
        
        if (restaurants.size() != 1) { //No existeix
            db.store(pRestaurant);
            db.commit();
        } else { //Existeix
            restaurant = restaurants.iterator().next();
            restaurant.setCodi(pRestaurant.getCodi());
            restaurant.setNom(pRestaurant.getNom());
            restaurant.setAdreca(pRestaurant.getAdreca());
            restaurant.setElements(pRestaurant.getElements());
            db.store(restaurant);
            db.commit();
        }
        
        tancaConnexio();

    }

    /*
     *TODO
     * 
     *Paràmetres: el nom del fitxer on està guardat el restaurant
     *
     *Acció:
     *  - Estableixeu la connexio i al final tanqueu-la
     *  - Heu de carregar el restaurant des de la base de dades assignant-lo a l'atribut restaurant.
     *    Si no existeix, llanceu l'excepció GestioReservesExcepcio amb codi  "GestorDB4O.noExisteix"
     *  - S'ha de fer la consulta amb Predicate
     *
     *Retorn: cap
     *
     */
    @Override
    public void carregarRestaurant(String nomFitxer) throws GestioReservesExcepcio {
 
        estableixConnexio();
        final int codi = Integer.parseInt(nomFitxer);

        List<Restaurant> restaurants = db.query(new Predicate<Restaurant>() {
            public boolean match(Restaurant unRestaurant) {
                return unRestaurant.getCodi()==codi;
            }
        });
        
        if (restaurants.size() != 1) { //No existeix
            throw new GestioReservesExcepcio("GestorDB4O.noExisteix");
        } else {
            restaurant = restaurants.iterator().next();
        }
        
        tancaConnexio();

    }
}
