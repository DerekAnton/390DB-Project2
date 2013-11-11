import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {
    private static Properties configProps = new Properties();

    private static String imdbUrl;
    private static String customerUrl;

    private static String postgreSQLDriver;
    private static String postgreSQLUser;
    private static String postgreSQLPassword;

    // DB Connection
    private Connection _imdb;
    private Connection _customer_db;

    // Canned queries

    private String _search_sql = "SELECT * FROM movie WHERE name COLLATE Latin1_General_CS_AS like ? ORDER BY id";
    private PreparedStatement _search_statement;

    private String _director_mid_sql = "SELECT y.* "
                     + "FROM movie_directors x, directors y "
                     + "WHERE x.mid = ? and x.did = y.id";
					 
	private String _actor_mid_sql = "SELECT y.* "
                     + "FROM casts x, actor y "
                     + "WHERE x.mid = ? and x.pid = y.id";
					 
	private String _customer_name_sql = "SELECT lname, fname"
                     + "FROM  Customers "
                     + "WHERE cid = ?";
	private String _remaining_rental_sql = "SELECT r.max_movies - curRental.num "
                     + "FROM Customers c, RentalPlans r, (Select count(*) AS num from MovieRentals m where m.cid = ? AND m.status='open') curRental  "
                     + "WHERE c.cid = ? AND c.pid = r.pid";
	
	private String _who_has_this_movie_sql = "SELECT c.cid"
                     + "FROM  Customers c, MovieRentals m"
                     + "WHERE cid = m.cid AND m.mid = ? AND m.status = 'open'";
	
    private String _rental_plans_sql = 
    		"SELECT *" +
    		"FROM rentalplan";
	
    private PreparedStatement _director_mid_statement;
	private PreparedStatement _actor_mid_statement;
	private PreparedStatement _customer_name_statement;
	private PreparedStatement _remaining_rental_statement;
	private PreparedStatement _who_has_this_movie_statement;
    private PreparedStatement _rental_plans_statement;

    /* uncomment, and edit, after your create your own customer database */
    /*
    private String _customer_login_sql = "SELECT * FROM customers WHERE login = ? and password = ?";
    private PreparedStatement _customer_login_statement;

    private String _begin_transaction_read_write_sql = "BEGIN TRANSACTION READ WRITE";
    private PreparedStatement _begin_transaction_read_write_statement;

    private String _commit_transaction_sql = "COMMIT TRANSACTION";
    private PreparedStatement _commit_transaction_statement;

    private String _rollback_transaction_sql = "ROLLBACK TRANSACTION";
    private PreparedStatement _rollback_transaction_statement;
     */

    public Query() {
    }

    /**********************************************************/
    /* Connections to postgres databases */

    public void openConnection() throws Exception {
        configProps.load(new FileInputStream("dbconn.config"));
        
        
        imdbUrl        = configProps.getProperty("imdbUrl");
        customerUrl    = configProps.getProperty("customerUrl");
        postgreSQLDriver   = configProps.getProperty("postgreSQLDriver");
        postgreSQLUser     = configProps.getProperty("postgreSQLUser");
        postgreSQLPassword = configProps.getProperty("postgreSQLPassword");


        /* load jdbc drivers */
        Class.forName(postgreSQLDriver).newInstance();

        /* open connections to TWO databases: imdb and the customer database */
        _imdb = DriverManager.getConnection(imdbUrl, // database
                postgreSQLUser, // user
                postgreSQLPassword); // password

        _customer_db = DriverManager.getConnection(customerUrl, // database
                postgreSQLUser, // user
                postgreSQLPassword); // password
    }

    public void closeConnection() throws Exception {
        _imdb.close();
        _customer_db.close();
    }

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

    public void prepareStatements() throws Exception {

        _search_statement = _imdb.prepareStatement(_search_sql);
        _director_mid_statement = _imdb.prepareStatement(_director_mid_sql);
		_actor_mid_statement = _imdb.prepareStatement(_actor_mid_sql);
		_customer_name_statement = _customer_db.prepareStatement(_customer_name_sql);
		_remaining_rental_statement = _customer_db.prepareStatement(_remaining_rental_sql);
		 _who_has_this_movie_statement = _customer_db.prepareStatement( _who_has_this_movie_sql);
		 _rental_plans_statement = _customer_db.prepareStatement(_rental_plans_sql);
        /* uncomment after you create your customers database */
        /*
        _customer_login_statement = _customer_db.prepareStatement(_customer_login_sql);
        _begin_transaction_read_write_statement = _customer_db.prepareStatement(_begin_transaction_read_write_sql);
        _commit_transaction_statement = _customer_db.prepareStatement(_commit_transaction_sql);
        _rollback_transaction_statement = _customer_db.prepareStatement(_rollback_transaction_sql);
         */

        /* add here more prepare statements for all the other queries you need */
        /* . . . . . . */
    }


    /**********************************************************/
    /* suggested helper functions  */

    public int helper_compute_remaining_rentals(int cid) throws Exception {
        /* how many movies can she/he still rent ? */
        /* you have to compute and return the difference between the customer's plan
           and the count of oustanding rentals */
		   _remaining_rental_statement.clearParameters();
		   _remaining_rental_statement.setInt(1,cid);
		   ResultSet remainingNum = _remaining_rental_statement.executeQuery();
        return remainingNum.getInt(1);
    }

    public String helper_compute_customer_name(int cid) throws Exception {
        /* you find  the first + last name of the current customer */
		_customer_name_statement.clearParameters();
        _customer_name_statement.setInt(1, cid);
        ResultSet name_set = _customer_name_statement.executeQuery();
		String name = name_set.getString(2) + " " + name_set.getString(1);
		return name;
    }
    

    public boolean helper_check_plan(int plan_id) throws Exception {
        /* is plan_id a valid plan id ?  you have to figure out */
    	
        return true;
    }

    public boolean helper_check_movie(int mid) throws Exception {
        /* is mid a valid movie id ? you have to figure out  */
        return true;
    }

    private int helper_who_has_this_movie(int mid) throws Exception {
        /* find the customer id (cid) of whoever currently rents the movie mid; return -1 if none */
		 _who_has_this_movie_statement.clearParameters();
		  _who_has_this_movie_statement.setInt(1,mid);
		  ResultSet cid =  _who_has_this_movie_statement.executeQuery();
		  if(cid.next()){
			return cid.getInt(1);
		  }
		  else{
		  return -1;
		  }
    }

    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
    public int transaction_login(String name, String password) throws Exception {
        /* authenticates the user, and returns the user id, or -1 if authentication fails */

        /* Uncomment after you create your own customers database */
        /*
        int cid;

        _customer_login_statement.clearParameters();
        _customer_login_statement.setString(1,name);
        _customer_login_statement.setString(2,password);
        ResultSet cid_set = _customer_login_statement.executeQuery();
        if (cid_set.next()) cid = cid_set.getInt(1);
        else cid = -1;
        return(cid);
         */
        return (55);
    }

    public void transaction_personal_data(int cid) throws Exception {
        /* println the customer's personal data: name, and plan number */
        System.out.println("Name: " + helper_compute_customer_name(cid));
        System.out.println("You can rent " + helper_compute_remaining_rentals(cid) + " additional movies");
    }


    /**********************************************************/
    /* main functions in this project: */

    public void transaction_search(int cid, String movie_title)
            throws Exception {
        /* searches for movies with matching titles: SELECT * FROM movie WHERE name LIKE movie_title */
        /* prints the movies, directors, actors, and the availability status:
           AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */

        /* set the first (and single) '?' parameter */
        _search_statement.clearParameters();
        _search_statement.setString(1, '%' + movie_title + '%');

        ResultSet movie_set = _search_statement.executeQuery();
        while (movie_set.next()) {
            int mid = movie_set.getInt(1);
            System.out.println("ID: " + mid + " NAME: "
                    + movie_set.getString(2) + " YEAR: "
                    + movie_set.getString(3));
            /* do a dependent join with directors */
            _director_mid_statement.clearParameters();
            _director_mid_statement.setInt(1, mid);
            ResultSet director_set = _director_mid_statement.executeQuery();
            while (director_set.next()) {
                System.out.println("\t\tDirector: " + director_set.getString(3)
                        + " " + director_set.getString(2));
            }
            director_set.close();
            /* now you need to retrieve the actors, in the same manner */
			
			_actor_mid_statement.clearParameters();
            _actor_mid_statement.setInt(1, mid);
            ResultSet actor_set = _actor_mid_statement.executeQuery();
            while (actor_set.next()) {
                System.out.println("\t\tActor: " + actor_set.getString(3)
                        + " " + actor_set.getString(2) + " " + actor_set.getString(4) );
            }
            actor_set.close();
            /* then you have to find the status: of "AVAILABLE" "YOU HAVE IT", "UNAVAILABLE" */
			if(helper_who_has_this_movie(mid) == cid){
				System.out.println("YOU HAVE IT");
			}
			else if((helper_compute_remaining_rentals(cid) == 0) || helper_who_has_this_movie(mid) != -1){
				System.out.println("UNAVAILABLE");
			}
			
			else if(helper_who_has_this_movie(mid) == -1){
				System.out.println("AVAILABLE");
			}
        }
        System.out.println();
    }

    public void transaction_choose_plan(int cid, int pid) throws Exception {
        /* updates the customer's plan to pid: UPDATE customers SET plid = pid */
        /* remember to enforce consistency ! */
    }

    public void transaction_list_plans() throws Exception {
        /* println all available plans: SELECT * FROM plan */
    	System.out.println("List of plans: ");
    	_rental_plans_statement.clearParameters();
        ResultSet plan_set = _rental_plans_statement.executeQuery();
        
        while(plan_set.next()){
        	System.out.print("Plan ID: " + plan_set.getInt(1) + ", ");
        	System.out.print("Name: " + plan_set.getString(2) + ", ");
        	System.out.print("Max Rentals: " + plan_set.getInt(3) + ", ");
        	System.out.print("Monthly Fee: " + plan_set.getDouble(4) + "\n");
        }
    }
    
    public void transaction_list_user_rentals(int cid) throws Exception {
        /* println all movies rented by the current user*/
    }

    public void transaction_rent(int cid, int mid) throws Exception {
        /* rend the movie mid to the customer cid */
        /* remember to enforce consistency ! */
    }

    public void transaction_return(int cid, int mid) throws Exception {
        /* return the movie mid by the customer cid */
    }

    public void transaction_fast_search(int cid, String movie_title)
            throws Exception {
                    	HashMap<String, StringBuilder> results = new HashMap<String, StringBuilder>();
    	//sloppy code but it works.
    	//i'll clean it up a bit and double check it again in the next few days.
    	//-Andrew
    	StringBuilder current;
    	ResultSet qResults = null;
    	String id;
    	while( 
    		(qResults==null?(qResults = this._imdb.createStatement().executeQuery(
    			"SELECT * " +
    			"FROM MOVIE " +
    			"WHERE NAME LIKE '%" + movie_title + "%' " +
    			"ORDER BY id;"
    			)):qResults).next()){
    			current = new StringBuilder();
    			
    			id = qResults.getString("id");
    			current.append("ID : " + id +
    						   "\nName : " + qResults.getString("name") +
    						   "\nYear : " + qResults.getString("year") +
    						   "\n");
    			results.put(id, current);
    	} qResults = null;
    	
    	while(
    		(qResults==null?(qResults = this._imdb.createStatement().executeQuery(
    			"SELECT m.id, d.fname, d.lname " +
    			"FROM MOVIE m " +
    			"INNER JOIN MOVIE_DIRECTORS md ON m.id=mid " +
    			"INNER JOIN DIRECTORS d ON md.did=d.id " +
    			"WHERE NAME LIKE '%" + movie_title + "%' " +
    			"ORDER BY m.id;"
    			)):qResults).next()){
    		
    			current = results.get(qResults.getString("id"));
    			current.append("Director : " + qResults.getString("fname") + 
    						   ' '		   	 + qResults.getString("lname") +
    						   "\n");
    	} qResults = null;
    	
    	while(
    		(qResults==null?(qResults = this._imdb.createStatement().executeQuery(
    			"SELECT m.id, a.fname, a.lname " +
    			"FROM MOVIE m " +
    			"INNER JOIN CASTS c ON m.id=c.mid " +
    			"INNER JOIN ACTOR a on a.id=c.pid " +
    			"WHERE NAME LIKE '%" + movie_title + "%' " +
    			"ORDER BY m.id;"
    			)):qResults).next()){
    		current = results.get(qResults.getString("id"));
    		current.append("Actor : " + qResults.getString("fname") +
    					   " " 		  + qResults.getString("lname") + 
    					   "\n");
    	} qResults = null;
    	for(StringBuilder sb : results.values())
    		System.out.println(sb.toString());
    }

}
