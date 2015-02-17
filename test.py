import unittest
import netcrappy


class TestFiler(netcrappy.Filer):

    """Docstring for TestFiler. """

    def __init__(self):
        """TODO: to be defined1. """
        netcrappy.Filer.__init__(self)

    def dummy_invoke(self, *invoke_args):
        """TODO: Docstring for dummy_invoke.

        :*invoke_args: TODO
        :returns: TODO

        """
        pass 

class TestDictToNaElement(unittest.TestCase):
    def setUp(self):
        self.testdict = {'results': 
                    {'volumes': 
                     {'volume-info': [
                         {'name': 'vol1'}, 
                         {'name': 'vol2'}
                     ]
                     }
                    },
                    'attrs': {'status': 'passed'}
                   }

    def test_dict_to_nalelem(self):
        test_naelem = netcrappy.ontap7mode.dict_to_naelement(self.testdict)
        expected_ouput = ('<results status="passed">\n\t'
                          '<volumes>\n\t\t'
                          '<volume-info>\n\t\t\t'
                          '<name>vol1</name>\n\t\t'
                          '</volume-info>\n\t\t'
                          '<volume-info>\n\t\t\t'
                          '<name>vol2</name>\n\t\t'
                          '</volume-info>\n\t'
                          '</volumes>\n'
                          '</results>\n')
        self.assertEqual(test_naelem.sprintf(), expected_ouput)

if __name__ == "__main__":
    unittest.main()
