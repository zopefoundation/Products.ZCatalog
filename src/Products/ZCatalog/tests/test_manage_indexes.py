import Testing.testbrowser
import Testing.ZopeTestCase
import Zope2.App


class IndexSortingTests(Testing.ZopeTestCase.FunctionalTestCase):
    """Browser testing /manage_catalogIndexes"""

    def setUp(self):
        super().setUp()

        Zope2.App.zcml.load_site(force=True)

        uf = self.app.acl_users
        uf.userFolderAddUser('manager', 'manager_pass', ['Manager'], [])
        zcatalog = self.app.manage_addProduct['ZCatalog']
        zcatalog.manage_addZCatalog('catalog', 'The Catalog')
        pli = self.app.catalog.Indexes.manage_addProduct['PluginIndexes']
        pli.manage_addFieldIndex('Index1')
        pli.manage_addKeywordIndex('Index2')
        self.browser = Testing.testbrowser.Browser()
        self.browser.login('manager', 'manager_pass')

    def check_order(self, expect_1_before_2):
        index1_pos = self.browser.contents.find('Index1')
        index2_pos = self.browser.contents.find('Index2')
        found_1_before_2 = index2_pos > index1_pos
        self.assertEqual(found_1_before_2, expect_1_before_2)

    def test_sortby(self):
        base_url = (
            'http://localhost/catalog/manage_catalogIndexes?skey=%s&rkey=%s')

        self.browser.open(base_url % ('id', 'asc'))
        self.check_order(expect_1_before_2=True)

        self.browser.open(base_url % ('id', 'desc'))
        self.check_order(expect_1_before_2=False)

        self.browser.open(base_url % ('meta_type', 'asc'))
        self.check_order(expect_1_before_2=True)

        self.browser.open(base_url % ('meta_type', 'desc'))
        self.check_order(expect_1_before_2=False)
