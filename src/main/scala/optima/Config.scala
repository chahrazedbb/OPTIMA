package optima

import com.typesafe.config.ConfigFactory

class Config {

}
object Config {

    def get(key: String): String = {

        val value = ConfigFactory.load().getString(key)
        return value
    }
}
