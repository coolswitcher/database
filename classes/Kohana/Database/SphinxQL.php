<?php defined('SYSPATH') OR die('No direct script access.');
/**
 * SphinxQL database connection.
 *
 * @package    Kohana/Database
 * @category   Drivers
 * @author     CoolSwitcher
 */
class Kohana_Database_SphinxQL extends Database {

	// Database in use by each connection
	protected static $_current_databases = array();

	// Use SET NAMES to set the character set
	protected static $_set_names;

	// Identifier for this connection within the PHP driver
	protected $_connection_id;

	// MySQL uses a backtick for identifiers
	protected $_identifier = '';

	public function connect()
	{
		if ($this->_connection)
			return;

		// Extract the connection parameters, adding required variabels

		extract($this->_config['connection'] + array(
			'hostname' => '127.0.0.1',
			'port'     => 9306,
		));

		try
		{
			$this->_connection = new mysqli($hostname, NULL, NULL, NULL, $port, NULL);
		}
		catch (Exception $e)
		{
			// No connection exists
			$this->_connection = NULL;

			throw new Database_Exception(':error', array(':error' => $e->getMessage()), $e->getCode());
		}

		// \xFF is a better delimiter, but the PHP driver uses underscore
		$this->_connection_id = sha1($hostname.'_'.$port);
	}

	public function disconnect()
	{
		try
		{
			// Database is assumed disconnected
			$status = TRUE;

			if (is_resource($this->_connection))
			{
				if ($status = $this->_connection->close())
				{
					// Clear the connection
					$this->_connection = NULL;

					// Clear the instance
					parent::disconnect();
				}
			}
		}
		catch (Exception $e)
		{
			// Database is probably not disconnected
			$status = ! is_resource($this->_connection);
		}

		return $status;
	}

	public function set_charset($charset)
	{
		// Make sure the database is connected
		$this->_connection or $this->connect();

		if (Database_SphinxQL::$_set_names === TRUE)
		{
			// PHP is compiled against MySQL 4.x
			$status = (bool) $this->_connection->query('SET NAMES '.$this->quote($charset));
		}
		else
		{
			// PHP is compiled against MySQL 5.x
			$status = $this->_connection->set_charset($charset);
		}

		if ($status === FALSE)
		{
			throw new Database_Exception(':error', array(':error' => $this->_connection->error), $this->_connection->errno);
		}
	}

	public function query($type, $sql, $as_object = FALSE, array $params = NULL)
	{
		$sql .= " option max_matches=" . Arr::get($this->_config, 'max_matches', 1000);
		// Make sure the database is connected
		$this->_connection or $this->connect();

		if (Kohana::$profiling)
		{
			// Benchmark this query for the current instance
			$benchmark = Profiler::start("Database ({$this->_instance})", $sql);
		}

		// Execute the query
		if (($result = $this->_connection->query($sql)) === FALSE)
		{
			if (isset($benchmark))
			{
				// This benchmark is worthless
				Profiler::delete($benchmark);
			}

			throw new Database_Exception(':error [ :query ]', array(
				':error' => $this->_connection->error,
				':query' => $sql
			), $this->_connection->errno);
		}

		if (isset($benchmark))
		{
			Profiler::stop($benchmark);
		}

		// Set the last query
		$this->last_query = $sql;

		if ($type === Database::SELECT)
		{
			// Return an iterator of results
			return new Database_MySQLi_Result($result, $sql, $as_object, $params);
		}
		elseif ($type === Database::INSERT)
		{
			// Return a list of insert id and rows created
			return array(
				$this->_connection->insert_id,
				$this->_connection->affected_rows,
			);
		}
		else
		{
			// Return the number of rows affected
			return $this->_connection->affected_rows;
		}
	}

	/**
	 * Start a SQL transaction
	 *
	 * @link http://dev.mysql.com/doc/refman/5.0/en/set-transaction.html
	 *
	 * @param string $mode  Isolation level
	 * @return boolean
	 */
	public function begin($mode = NULL)
	{
		throw new Database_Exception(':error', array(':error' => 'Transactions not suported'), 0);
	}

	/**
	 * Commit a SQL transaction
	 *
	 * @throws Database_Exception
	 */
	public function commit()
	{
		throw new Database_Exception(':error', array(':error' => 'Transactions not suported'), 0);
	}

	/**
	 * Rollback a SQL transaction
	 * @throws Database_Exception
	 */
	public function rollback()
	{
		throw new Database_Exception(':error', array(':error' => 'Transactions not suported'), 0);
	}

	public function list_tables($like = NULL)
	{
		return FALSE;
	}

	public function list_columns($table, $like = NULL, $add_prefix = TRUE)
	{
		return FALSE;
	}

	/**
	 * Sphinx specific escaping
	 * @param  mixed $value
	 * @param  bool $quotes
	 * @return mixed
	 */
	public function escape($value, $quotes = TRUE)
	{
	    if ( is_numeric($value) OR is_null ($value))
	    {
	    	return $value; 
	    }
	    
	    $from = array ('\\', '(',')','|','-','!','@','~','"','&', '/', '^', '$', '=', "'", "\x00", "\n", "\r", "\x1a" );
	    $to   = array ('\\\\', '\\\(','\\\)','\\\|','\\\-','\\\!','\\\@','\\\~','\\\"', '\\\&', '\\\/', '\\\^', '\\\$', '\\\=', "\\'", "\\x00", "\\n", "\\r", "\\x1a");
	    $value = str_replace ($from, $to, $value);

		if ($quotes)
		{
			return "'$value'";
		}
		else
		{
			return $value;
		}
	}

} // End Database_SphinxQL
