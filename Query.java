/* Group E CS390DB Fall 2013 */
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

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
	private String _search_sql = "SELECT * FROM movie WHERE name ILIKE ? ORDER BY id";
	private PreparedStatement _search_statement;

	private String _director_mid_sql = "SELECT d.* "
			+ "FROM movie_directors r, directors d "
			+ "WHERE r.mid = ? AND r.did = d.id";
	private PreparedStatement _director_mid_statement;

	/* custom statements */
	private String _actor_mid_sql = "SELECT a.* " + "FROM casts c, actor a "
			+ "WHERE c.mid = ? and c.pid = a.id";
	private PreparedStatement _actor_mid_statement;

	private String _customer_name_sql = "SELECT lname, fname "
			+ "FROM customer " + "WHERE cust_id = ?";
	private PreparedStatement _customer_name_statement;

	private String _remaining_rental_sql = "SELECT r.maxrentals - curRental.num "
			+ "FROM customer c, rentalplan r, (SELECT COUNT(*) AS num FROM activerental m WHERE m.cust_id = ?) curRental "
			+ "WHERE c.cust_id = ? AND c.plan_id = r.plan_id";
	private PreparedStatement _remaining_rental_statement;

	private String _who_has_this_movie_sql = "SELECT cust_id "
			+ "FROM activerental " + "WHERE movie_id = ?";
	private PreparedStatement _who_has_this_movie_statement;

	private String _rental_plans_sql = "SELECT * " + "FROM rentalplan";
	private PreparedStatement _rental_plans_statement;

	private String _rentals_for_plan_sql = "SELECT maxrentals FROM rentalplan WHERE plan_id = ?";
	private PreparedStatement _rentals_for_plan_statement;

	private String _update_rental_plan_sql = "UPDATE customer "
			+ "SET plan_id = ? " + "WHERE cust_id = ?";
	private PreparedStatement _update_rental_plan_statement;

	private String _rent_mid_to_cid_sql = "INSERT INTO activerental (movie_id, cust_id, dateout) VALUES (?, ?, current_timestamp)";
	private PreparedStatement _rent_mid_to_cid_statement;

	private String _activerentals_by_cid_sql = "SELECT * FROM activerental WHERE cust_id = ?";
	private PreparedStatement _activerentals_by_cid_statement;

	private String _activerentals_count_sql = "SELECT count(*) FROM activerental WHERE cust_id = ?";
	private PreparedStatement _activerentals_count_statement;

	/**
	 * don't need cid when mid is unique, triggers move deleted to historical
	 * inactiverental table
	 */
	private String _return_by_mid_sql = "DELETE FROM activerental WHERE movie_id = ?";
	private PreparedStatement _return_by_mid_statement;

	private String _movie_join_dir_sql = "SELECT m.id, d.fname, d.lname FROM movie m INNER JOIN movie_directors md "
			+ "ON m.id=md.mid INNER JOIN directors d ON md.did=d.id WHERE name ILIKE ? ORDER BY m.id";
	private PreparedStatement _movie_join_dir_statement;

	private String _movie_join_actor_sql = "SELECT m.id, a.fname, a.lname FROM movie m INNER JOIN casts c "
			+ "ON m.id=c.mid INNER JOIN actor a on a.id=c.pid WHERE name ILIKE ? ORDER BY m.id";
	private PreparedStatement _movie_join_actor_statement;

	private String _customer_login_sql = "SELECT * FROM customer WHERE username = ? and password = ?";
	private PreparedStatement _customer_login_statement;

	private String _begin_transaction_read_write_sql = "BEGIN TRANSACTION READ WRITE";
	private PreparedStatement _begin_transaction_read_write_statement;

	private String _commit_transaction_sql = "COMMIT TRANSACTION";
	private PreparedStatement _commit_transaction_statement;

	private String _rollback_transaction_sql = "ROLLBACK TRANSACTION";
	private PreparedStatement _rollback_transaction_statement;

	private String _movie_by_id_sql = "SELECT * FROM movie WHERE id = ?";
	private PreparedStatement _movie_by_id_statement;

	private ArrayList<PreparedStatement> openStatements;

	public Query() {
		openStatements = new ArrayList<PreparedStatement>(10);
	}

	/**********************************************************/
	/* Connections to postgres databases */

	public void openConnection() throws Exception {
		configProps.load(new FileInputStream("dbconn.config"));

		imdbUrl = configProps.getProperty("imdbUrl");
		customerUrl = configProps.getProperty("customerUrl");
		postgreSQLDriver = configProps.getProperty("postgreSQLDriver");
		postgreSQLUser = configProps.getProperty("postgreSQLUser");
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
		for (PreparedStatement stm : openStatements) {
			stm.close();
		}
		_imdb.close();
		_customer_db.close();
	}

	/**********************************************************/
	/**
	 * keep track of opens statements so we can close them before closing
	 * connection
	 *
	 * @throws SQLException
	 */
	private PreparedStatement openStatement(Connection conn, String sql)
			throws SQLException {
		PreparedStatement stm = conn.prepareStatement(sql);
		openStatements.add(stm);
		return stm;
	}

	/**
	 * prepare all the SQL statements in this method. "preparing" a statement is
	 * almost like compiling it. Note that the parameters (with ?) are still not
	 * filled in
	 */
	public void prepareStatements() throws Exception {

		_search_statement = openStatement(_imdb, _search_sql);
		_director_mid_statement = openStatement(_imdb, _director_mid_sql);
		/* custom statements */
		_actor_mid_statement = openStatement(_imdb, _actor_mid_sql);
		_customer_name_statement = openStatement(_customer_db,
				_customer_name_sql);
		_remaining_rental_statement = openStatement(_customer_db,
				_remaining_rental_sql);
		_who_has_this_movie_statement = openStatement(_customer_db,
				_who_has_this_movie_sql);
		_rental_plans_statement = openStatement(_customer_db, _rental_plans_sql);
		_rentals_for_plan_statement = openStatement(_customer_db,
				_rentals_for_plan_sql);
		_update_rental_plan_statement = openStatement(_customer_db,
				_update_rental_plan_sql);
		_movie_by_id_statement = openStatement(_imdb, _movie_by_id_sql);
		_rent_mid_to_cid_statement = openStatement(_customer_db,
				_rent_mid_to_cid_sql);
		_activerentals_by_cid_statement = openStatement(_customer_db,
				_activerentals_by_cid_sql);
		_activerentals_count_statement = openStatement(_customer_db,
				_activerentals_count_sql);
		_return_by_mid_statement = openStatement(_customer_db,
				_return_by_mid_sql);
		_movie_join_dir_statement = openStatement(_imdb, _movie_join_dir_sql);
		_movie_join_actor_statement = openStatement(_imdb,
				_movie_join_actor_sql);
		_customer_login_statement = openStatement(_customer_db,
				_customer_login_sql);
		_begin_transaction_read_write_statement = openStatement(_customer_db,
				_begin_transaction_read_write_sql);
		_commit_transaction_statement = openStatement(_customer_db,
				_commit_transaction_sql);
		_rollback_transaction_statement = openStatement(_customer_db,
				_rollback_transaction_sql);
	}

	/**********************************************************/
	/* suggested helper functions */
	/**
	 * how many movies can she/he still rent ? you have to compute and return
	 * the difference between the customer's plan and the count of outstanding
	 * rentals
	 *
	 * @param cid
	 * @return
	 * @throws Exception
	 */
	public int helper_compute_remaining_rentals(int cid) throws Exception {

		_remaining_rental_statement.clearParameters();
		_remaining_rental_statement.setInt(1, cid);
		_remaining_rental_statement.setInt(2, cid);

		ResultSet remainingNum = _remaining_rental_statement.executeQuery();
		remainingNum.next();

		return remainingNum.getInt(1);
	}

	/**
	 * you find the first + last name of the current customer
	 *
	 * @param cid
	 * @return
	 * @throws Exception
	 */
	public String helper_compute_customer_name(int cid) throws Exception {
		_customer_name_statement.clearParameters();
		_customer_name_statement.setInt(1, cid);
		ResultSet name_set = _customer_name_statement.executeQuery();
		name_set.next();
		String name = name_set.getString(2) + " " + name_set.getString(1);
		return name;
	}

	/**
	 * is plan_id a valid plan id ? you have to figure out
	 *
	 * @param plan_id
	 * @return
	 * @throws Exception
	 */
	public boolean helper_check_plan(int plan_id) throws Exception {
		ResultSet rp = null;
		try {
			_rentals_for_plan_statement.clearParameters();
			_rentals_for_plan_statement.setInt(1, plan_id);
			rp = _rentals_for_plan_statement.executeQuery();
			return rp.next();
		} finally {
			rp.close();
		}
	}

	/**
	 * is mid a valid movie id ? you have to figure out
	 *
	 * @param mid
	 * @return
	 * @throws Exception
	 */
	public boolean helper_check_movie(int mid) throws Exception {
		ResultSet movie = null;
		try {
			_movie_by_id_statement.clearParameters();
			_movie_by_id_statement.setInt(1, mid);
			movie = _movie_by_id_statement.executeQuery();
			return movie.next();
		} finally {
			movie.close();
		}
	}

	/**
	 * find the customer id (cid) of whoever currently rents the movie mid;
	 * return -1 if none
	 *
	 * @param mid
	 * @return
	 * @throws Exception
	 */
	private int helper_who_has_this_movie(int mid) throws Exception {
		_who_has_this_movie_statement.clearParameters();
		_who_has_this_movie_statement.setInt(1, mid);
		ResultSet cid = _who_has_this_movie_statement.executeQuery();
		if (cid.next()) {
			return cid.getInt(1);
		} else {
			return -1;
		}
	}

	/**********************************************************/
	/**
	 * login transaction: invoked only once, when the app is started
	 * authenticates the user, and returns the user id, or -1 if authentication
	 * fails
	 *
	 * @param name
	 * @param password
	 * @return
	 * @throws Exception
	 */
	public int transaction_login(String name, String password) throws Exception {
		int cid;

		_customer_login_statement.clearParameters();
		_customer_login_statement.setString(1, name);
		_customer_login_statement.setString(2, password);
		ResultSet cid_set = _customer_login_statement.executeQuery();
		if (cid_set.next())
			cid = cid_set.getInt(1);
		else
			cid = -1;
		return (cid);
	}

	/**
	 * println the customer's personal data: name, and plan number
	 *
	 * @param cid
	 * @throws Exception
	 */
	public void transaction_personal_data(int cid) throws Exception {
		System.out.println("Name: " + helper_compute_customer_name(cid));
		System.out.println("You can rent "
				+ helper_compute_remaining_rentals(cid) + " additional movies");
	}

	/**********************************************************/
	/* main functions in this project: */

	/**
	 * searches for movies with matching titles: SELECT * FROM movie WHERE name
	 * LIKE movie_title prints the movies, directors, actors, and the
	 * availability status: AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT
	 * set the first (and single) '?' parameter
	 *
	 * @param cid
	 * @param movie_title
	 * @throws Exception
	 */
	public void transaction_search(int cid, String movie_title)
			throws Exception {
		_search_statement.clearParameters();
		_search_statement.setString(1, "%" + movie_title + "%");
		ResultSet movie_set = null;
		StringBuilder sb = new StringBuilder();
		try {
			movie_set = _search_statement.executeQuery();
			boolean empty = true;
			while (movie_set.next()) {
				empty = false;
				int mid = movie_set.getInt(1);
				sb.append(String.format("ID: %d\nName: %s\nYear: %s\n", mid,
						movie_set.getString(2), movie_set.getString(3)));
				/* do a dependent join with directors */
				_director_mid_statement.clearParameters();
				_director_mid_statement.setInt(1, mid);
				ResultSet director_set = _director_mid_statement.executeQuery();
				while (director_set.next()) {
					sb.append(String.format("Director: %s %s\n",
							director_set.getString(3),
							director_set.getString(2)));
				}
				director_set.close();
				/* now you need to retrieve the actors, in the same manner */
				_actor_mid_statement.clearParameters();
				_actor_mid_statement.setInt(1, mid);
				ResultSet actor_set = _actor_mid_statement.executeQuery();
				while (actor_set.next()) {
					sb.append(String.format("Actor: %s %s %s\n",
							actor_set.getString(3), actor_set.getString(2),
							actor_set.getString(4)));
				}
				actor_set.close();
				/*
				 * then you have to find the status: of "AVAILABLE"
				 * "YOU HAVE IT", "UNAVAILABLE"
				 */
				int has = helper_who_has_this_movie(mid);
				if (has == cid) {
					sb.append("YOU HAVE IT\n");
				} else if (helper_compute_remaining_rentals(cid) == 0
						|| has != -1) {
					sb.append("UNAVAILABLE\n");
				} else if (has == -1) {
					sb.append("AVAILABLE\n");
				}
			}
			if (empty) {
				sb.append("Not found.");
			}
		} finally {
			movie_set.close();
		}
		System.out.println(sb);
	}

	/**
	 * updates the customer's plan to pid: UPDATE customers SET plid = pid
	 * remember to enforce consistency
	 *
	 * @param cid
	 * @param pid
	 * @throws Exception
	 */
	public void transaction_choose_plan(int cid, int pid) throws Exception {
		_activerentals_count_statement.clearParameters();
		_activerentals_count_statement.setInt(1, cid);
		_rentals_for_plan_statement.clearParameters();
		_rentals_for_plan_statement.setInt(1, pid);
		_begin_transaction_read_write_statement.executeUpdate();
		_update_rental_plan_statement.clearParameters();
		_update_rental_plan_statement.setInt(1, pid);
		_update_rental_plan_statement.setInt(2, cid);
		ResultSet count_set = null;
		ResultSet maxRental_set = null;
		try {
			// retrieve the number of active rentals for that user
			count_set = _activerentals_count_statement.executeQuery();
			count_set.next();
			int activeRentals = count_set.getInt(1);
			// retrieve max allowed rentals for indicated plan
			maxRental_set = _rentals_for_plan_statement
					.executeQuery();
			maxRental_set.next();
			int maxRentals = maxRental_set.getInt(1);

			// compare active rentals with allowed rentals
			if (activeRentals <= maxRentals) {
				_update_rental_plan_statement.executeUpdate();
				_commit_transaction_statement.executeUpdate();
			} else {
				_rollback_transaction_statement.executeUpdate();
				System.err.println("Plan not changed! You have "
						+ activeRentals + " active rentals. "
						+ "The new plan allows for " + maxRentals
						+ " active rentals. "
						+ "Please return some rentals first.");
			}
		} finally {
			count_set.close();
			maxRental_set.close();
		}
	}

	/**
	 * println all available plans: SELECT * FROM plan
	 *
	 * @throws Exception
	 */
	public void transaction_list_plans() throws Exception {
		_rental_plans_statement.clearParameters();
		ResultSet plan_set = _rental_plans_statement.executeQuery();

		while (plan_set.next()) {
			System.out.print("Plan ID: " + plan_set.getInt(1) + ", ");
			System.out.print("Name: " + plan_set.getString(2) + ", ");
			System.out.print("Max Rentals: " + plan_set.getInt(3) + ", ");
			System.out.print("Monthly Fee: " + plan_set.getDouble(4) + "\n");
		}
	}

	/**
	 * println all movies rented by the current user
	 *
	 * @param cid
	 * @throws Exception
	 */
	public void transaction_list_user_rentals(int cid) throws Exception {
		ResultSet mids = null;
		ResultSet names = null;
		StringBuilder sb = new StringBuilder();
		sb.append("Your rentals:\n");
		try {
			_activerentals_by_cid_statement.clearParameters();
			_activerentals_by_cid_statement.setInt(1, cid);
			mids = _activerentals_by_cid_statement.executeQuery();
			while (mids.next()) {
				_movie_by_id_statement.clearParameters();
				// rental_id serial, movie_id integer, cust_id integer, dateout
				// timestamp
				_movie_by_id_statement.setInt(1, mids.getInt(2));
				names = _movie_by_id_statement.executeQuery();
				// id integer, name text, year integer
				if (names.next()) {
					sb.append(String.format("ID : %s\nName : %s\n",
							names.getString(1),
							names.getString(2)));
				} else {
					sb.append("Unknown\n"); // imdb movie somehow disappeared
				}
			}
		} finally {
			mids.close();
			if (names != null)
				names.close();
		}
		if (sb.length() <= 1) {
			sb.append("None\n");
		}
		System.out.println(sb);
	}

	/**
	 * rent the movie mid to the customer cid remember to enforce consistency !
	 *
	 * @param cid
	 * @param mid
	 * @throws Exception
	 */
	public void transaction_rent(int cid, int mid) throws Exception {
		if (helper_check_movie(mid)) {
			_rent_mid_to_cid_statement.clearParameters();
			_rent_mid_to_cid_statement.setInt(1, mid);
			_rent_mid_to_cid_statement.setInt(2, cid);
			_begin_transaction_read_write_statement.executeUpdate();
			if (helper_compute_remaining_rentals(cid) <= 0) {
				_rollback_transaction_statement.executeUpdate();
				System.err.println("Rental limit already reached.");
			} else if (helper_who_has_this_movie(mid) != -1) { // store only has
				// one copy
				_rollback_transaction_statement.executeUpdate();
				System.err.println("Movie out of stock");
			} else {
				_rent_mid_to_cid_statement.executeUpdate();
				_commit_transaction_statement.executeUpdate();
			}
		} else {
			System.err.println("Invalid movie ID.");
		}
	}

	/**
	 * return the movie mid by the customer cid
	 *
	 * @param cid
	 * @param mid
	 * @throws Exception
	 */
	public void transaction_return(int cid, int mid) throws Exception {
		if (helper_check_movie(mid)) {
			_return_by_mid_statement.clearParameters();
			_return_by_mid_statement.setInt(1, mid);
			_begin_transaction_read_write_statement.executeUpdate();
			// store only has one copy, don't let other users return movies for
			// others
			if (helper_who_has_this_movie(mid) != cid) {
				_rollback_transaction_statement.executeUpdate();
				System.err
						.println("Can only return movies you have checked out.");
			} else {
				_return_by_mid_statement.executeUpdate();
				_commit_transaction_statement.executeUpdate();
			}
		} else {
			System.err.println("Invalid movie ID.");
		}
	}

	/**
	 * like transaction_search, but uses joins instead of independent joins
	 * Needs to run three SQL queries: (a) movies, (b) movies join directors,
	 * (c) movies join actors Answers are sorted by mid. Then merge-joins the
	 * three answer sets
	 *
	 * @param cid
	 * @param movie_title
	 * @throws Exception
	 */
	public void transaction_fast_search(int cid, String movie_title)
			throws Exception {
		HashMap<String, StringBuilder> results = new HashMap<String, StringBuilder>();
		// cid is not required as a parameter.
		// fast search is not required to return rental status
		StringBuilder current;
		ResultSet qResults = null;
		String id;
		_search_statement.clearParameters();
		_search_statement.setString(1, "%" + movie_title + "%");
		boolean empty = true;
		while ((qResults == null ? (qResults = _search_statement
				.executeQuery()) : qResults).next()) {
			empty = false;
			current = new StringBuilder();
			id = qResults.getString("id");
			current.append(String.format("ID : %s\nName : %s\nYear : %s\n", id,
					qResults.getString("name"), qResults.getString("year")));
			results.put(id, current);
		}
		try {
			if (empty) {
				System.out.println("Not found.");
				return;
			}
		} finally {
			qResults.close();
			qResults = null;
		}
		_movie_join_dir_statement.clearParameters();
		_movie_join_dir_statement.setString(1, "%" + movie_title + "%");
		while ((qResults == null ? (qResults = _movie_join_dir_statement
				.executeQuery()) : qResults).next()) {
			current = results.get(qResults.getString("id"));
			current.append(String.format("Director : %s %s\n",
					qResults.getString("fname"), qResults.getString("lname")));
		}

		_movie_join_actor_statement.clearParameters();
		_movie_join_actor_statement.setString(1, "%" + movie_title + "%");
		qResults.close();
		qResults = null;
		while ((qResults == null ? (qResults = _movie_join_actor_statement
				.executeQuery()) : qResults).next()) {
			current = results.get(qResults.getString("id"));
			current.append(String.format("Actor : %s %s\n",
					qResults.getString("fname"), qResults.getString("lname")));
		}
		qResults.close();
		qResults = null;
		for (String mid : results.keySet()) {
			/*
			 * then you have to find the status: of "AVAILABLE"
			 * "YOU HAVE IT", "UNAVAILABLE"
			 */
			int has = helper_who_has_this_movie(Integer.parseInt(mid));
			if (has == cid) {
				results.get(mid).append("YOU HAVE IT\n");
			} else if (helper_compute_remaining_rentals(cid) == 0
					|| has != -1) {
				results.get(mid).append("UNAVAILABLE\n");
			} else if (has == -1) {
				results.get(mid).append("AVAILABLE\n");
			}
		}
		for (StringBuilder sb : results.values())
			System.out.println(sb.toString());
	}
}