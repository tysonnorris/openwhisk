package whisk.spi

import java.util.ServiceLoader

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import whisk.common.Logging
import whisk.core.WhiskConfig
//import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
/**
  * Created by tnorris on 6/8/17.
  */
trait Spi  extends Extension {
  //cannot assign config on construction since we use ServiceLoader below...
  protected[spi] var configVar:WhiskConfig = null
  protected[spi] var systemVar:ActorSystem = null
  protected[spi] var loggingVar:Logging = null
  protected def config:WhiskConfig = configVar
  protected def system:ActorSystem = systemVar
  protected def logging:Logging = loggingVar
}

abstract class SpiProvider[T <: Spi](configKey: String)(implicit tag: ClassTag[T]) extends ExtensionId[T] with ExtensionIdProvider {
  implicit val config:WhiskConfig
  implicit val logging:Logging
  override def apply(system: ActorSystem): T = {
    apply(system, null)
  }
  def apply(system: ActorSystem, initArgs:Any*): T = {
    system.registerExtension(this)
  }

  override def createExtension(system: ExtendedActorSystem): T = {
    createTyped(system)
  }

  private def createTyped(system: ExtendedActorSystem)(implicit tag: ClassTag[T]): T = {

    logging.info(this, s"creating new instance of ${tag.runtimeClass.getName}")
    val configuredImpl = system.settings.config.getString(configKey)

    if (configuredImpl != null) {
      //val spiImpl = (ServiceLoader load classOf[SpiType]).asScala
      val spiImpl = (ServiceLoader load tag.runtimeClass).asScala
      if (spiImpl.isEmpty) {
        throw new RuntimeException(s"No impls of ${tag.runtimeClass.getName} found in classpath; config key ${configKey} was expecting class ${configuredImpl}.")
      }
      logging.debug(this, s"ServiceLoader found ${spiImpl.size} impls of ${tag.runtimeClass.getName}")
      val availableImpls = spiImpl.filter(_.getClass.getName == configuredImpl)
      if (availableImpls.size == 0) {
        throw new IllegalStateException(s"No impl of ${tag.runtimeClass.getName} with type ${configuredImpl} found in classpath.")
      } else {
        if (availableImpls.size > 1) {
          logging.warn(this, s"Duplicate classes of type ${configuredImpl} found in classpath (will use firts).")
        }
        val resolved = availableImpls.head
        logging.info(this, s"Resolved spi impl for ${tag.runtimeClass.getName} to ${resolved.getClass.getName} using config key '${configKey}'.")
        val typedResolved = resolved.asInstanceOf[T]
        typedResolved.configVar = config
        typedResolved.systemVar = system
        typedResolved.loggingVar = logging
        typedResolved
      }
    } else {
      //no default and no override? return Nothing?
      throw new IllegalStateException(s"no default and no override config key ${configKey} to resolve impl for ${tag.runtimeClass.getName}")
    }
  }

  override def lookup(): ExtensionId[_ <: Extension] = {
    this
  }
}


