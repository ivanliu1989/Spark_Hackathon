package utilClasses
import utilClasses.modelTraining.train


/**
 * @author ivanliu
 */
object mainFile {
  def main(args: Array[String]) {
    val offer_path = "../data/offers"
    val train_path = "../data/trainHistory"
    val test_path = "../data/testHistory"
    val transaction_path = "../data/transactions"
    val (svmModel, lgModel) = train(offer_path, train_path, test_path, transaction_path)
  }
}